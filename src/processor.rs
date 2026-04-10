#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::{types::RawShred, TransactionEvent, TransactionHandler, UnshredConfig};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::Result;
use dashmap::DashSet;
use solana_entry::entry::Entry;
use solana_ledger::shred::{ReedSolomonCache, Shred, ShredType};
use std::{
    io::Cursor,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

// Header offsets inside a data shred's raw bytes
const OFFSET_FLAGS: usize = 85;
const OFFSET_SIZE: usize = 86; // Payload total size offset
const DATA_OFFSET_PAYLOAD: usize = 88;

/// A FEC set that's ready to be handed to the dispatcher.
#[derive(Debug)]
pub struct CompletedFecSet {
    pub slot: u64,
    /// Raw data shreds keyed by index. Already ordered-insertable by caller.
    pub data_shreds: HashMap<u32, Arc<RawShred>>,
}

struct FecSetAccumulator {
    slot: u64,
    data_shreds: HashMap<u32, Arc<RawShred>>,
    code_shreds: HashMap<u32, Arc<RawShred>>,
    expected_data_shreds: Option<usize>,
    created_at: Instant,
}

#[derive(Debug)]
enum ReconstructionStatus {
    NotReady,
    ReadyNatural,  // Have all data shreds, no FEC recovery needed
    ReadyRecovery, // Need FEC recovery but have enough shreds
}

#[derive(Debug)]
pub struct BatchWork {
    pub slot: u64,
    pub batch_start_idx: u32,
    pub batch_end_idx: u32,
    /// Raw data shreds for indices `[batch_start_idx, batch_end_idx]`.
    pub shreds: HashMap<u32, Arc<RawShred>>,
}

struct CombinedDataMeta {
    combined_data_shred_indices: Vec<usize>,
    combined_data_shred_received_at_micros: Vec<Option<u64>>,
    combined_data: Vec<u8>,
}

#[derive(Debug)]
pub struct EntryMeta {
    pub entry: Entry,
    pub received_at_micros: Option<u64>,
}

pub struct SlotAccumulator {
    data_shreds: HashMap<u32, Arc<RawShred>>,
    /// Indices of data shreds whose `data_flags & 0x40` bit is set and which
    /// are still awaiting dispatch. Kept sorted for O(log n) peek/drain.
    pending_batch_ends: std::collections::BTreeSet<u32>,
    last_processed_batch_idx: Option<u32>,
    created_at: Instant,
}

pub struct ShredProcessor {}

impl ShredProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run<H: TransactionHandler>(
        self,
        tx_handler: H,
        config: &UnshredConfig,
    ) -> Result<()> {
        let total_cores = num_cpus::get();

        // N dispatcher shards. FEC workers route completed sets by `slot % N`
        // so every FEC set of the same slot always lands on the same dispatcher.
        let num_dispatchers = config
            .num_dispatchers
            .map(|n| n as usize)
            .unwrap_or(2)
            .max(1);
        let (dispatch_senders, dispatch_receivers): (Vec<_>, Vec<_>) = (0..num_dispatchers)
            .map(|_| tokio::sync::mpsc::channel::<CompletedFecSet>(1000))
            .unzip();
        let dispatch_senders = Arc::new(dispatch_senders);

        // Track processed fec sets for deduplication
        let processed_fec_sets = Arc::new(DashSet::<(u64, u32)>::new());

        // Channels for receiver -> fec workers
        let num_fec_workers = match config.num_fec_workers {
            Some(num) => num,
            None => total_cores.saturating_sub(2) as u8,
        };
        let num_fec_workers = std::cmp::max(num_fec_workers, 1);
        let (shred_senders, shred_receivers): (Vec<_>, Vec<_>) = (0..num_fec_workers)
            .map(|_| tokio::sync::mpsc::channel::<RawShred>(10000))
            .unzip();

        // Spawn network receiver(s). Multiple receivers use SO_REUSEPORT so the
        // kernel hashes incoming packets across sockets by flow tuple.
        let bind_addr: std::net::SocketAddr = config.bind_address.parse()?;
        let num_receivers = config.num_receivers.map(|n| n as usize).unwrap_or(2).max(1);
        let receiver = crate::receiver::ShredReceiver::new(bind_addr, num_receivers)?;
        let receiver_handle =
            tokio::spawn(receiver.run(shred_senders, Arc::clone(&processed_fec_sets)));

        // Spawn fec workers
        info!(
            "Starting {} fec workers on {} cores",
            shred_receivers.len(),
            total_cores
        );
        let mut fec_handles = Vec::new();
        for (worker_id, fec_receiver) in shred_receivers.into_iter().enumerate() {
            let senders = Arc::clone(&dispatch_senders);
            let processed_fec_sets_clone = Arc::clone(&processed_fec_sets);

            let handle = tokio::spawn(async move {
                if let Err(e) = Self::run_fec_worker(
                    worker_id,
                    fec_receiver,
                    senders,
                    processed_fec_sets_clone,
                )
                .await
                {
                    error!("FEC worker {} failed: {}", worker_id, e);
                }
            });
            fec_handles.push(handle);
        }

        // Channels for batch dispatch worker -> batch processing workers
        let num_batch_workers = match config.num_batch_workers {
            Some(num) => num,
            None => total_cores.saturating_sub(3) as u8,
        };
        let num_batch_workers = std::cmp::max(num_batch_workers, 1);
        let (batch_senders, batch_receivers): (Vec<_>, Vec<_>) = (0..num_batch_workers)
            .map(|_| tokio::sync::mpsc::channel::<BatchWork>(10000))
            .unzip();

        // Spawn N dispatcher tasks — one per shard.
        info!("Starting {} dispatcher workers", num_dispatchers);
        let processor = Arc::new(self);
        let mut dispatch_handles = Vec::with_capacity(num_dispatchers);
        for (dispatcher_id, dispatch_receiver) in dispatch_receivers.into_iter().enumerate() {
            let senders = batch_senders.clone();
            let proc = Arc::clone(&processor);
            let processed_fec_sets_for_dispatch = Arc::clone(&processed_fec_sets);

            let handle = tokio::spawn(async move {
                if let Err(e) = proc
                    .dispatch_worker(
                        dispatcher_id,
                        dispatch_receiver,
                        senders,
                        processed_fec_sets_for_dispatch,
                    )
                    .await
                {
                    error!("Dispatcher {} failed: {:?}", dispatcher_id, e)
                }
            });
            dispatch_handles.push(handle);
        }
        // Drop our own copy so dispatchers see EOF when FEC workers finish.
        drop(dispatch_senders);

        // Spawn batch workers
        info!(
            "Starting {} batch workers on {} cores",
            num_batch_workers, total_cores
        );

        let tx_handler = Arc::new(tx_handler);
        let mut batch_handles = Vec::new();
        for (worker_id, batch_receiver) in batch_receivers.into_iter().enumerate() {
            let tx_handler_clone = Arc::clone(&tx_handler);

            let handle = tokio::spawn(async move {
                if let Err(e) =
                    Self::batch_worker(worker_id, batch_receiver, tx_handler_clone).await
                {
                    error!("Batch worker {} failed: {:?}", worker_id, e);
                }
            });
            batch_handles.push(handle);
        }

        // Wait for all workers to complete
        for handle in dispatch_handles {
            let _ = handle.await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), receiver_handle).await;
        for handle in fec_handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        drop(batch_senders);
        for handle in batch_handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        Ok(())
    }

    async fn run_fec_worker(
        worker_id: usize,
        mut receiver: Receiver<RawShred>,
        senders: Arc<Vec<Sender<CompletedFecSet>>>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        let reed_solomon_cache = Arc::new(ReedSolomonCache::default());
        let mut fec_set_accumulators: HashMap<(u64, u32), FecSetAccumulator> = HashMap::new();
        let mut last_cleanup = Instant::now();
        #[cfg(feature = "metrics")]
        let mut last_channel_udpate = Instant::now();

        loop {
            match receiver.recv().await {
                Some(raw) => {
                    if let Err(e) = Self::process_fec_shred(
                        raw,
                        &mut fec_set_accumulators,
                        &senders,
                        &reed_solomon_cache,
                        &processed_fec_sets,
                    )
                    .await
                    {
                        error!("FEC worker {} error: {:?}", worker_id, e);
                    }
                }
                None => {
                    warn!("FEC worker {} disconnected", worker_id);
                    break;
                }
            }

            #[cfg(feature = "metrics")]
            if last_channel_udpate.elapsed() > std::time::Duration::from_secs(1) {
                let capacity_used = receiver.len() as f64 / receiver.capacity() as f64 * 100.0;

                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .channel_capacity_utilization
                        .with_label_values(&[&format!("receiver_fec-worker_{}", worker_id)])
                        .set(capacity_used as i64);
                }

                last_channel_udpate = std::time::Instant::now();
            }

            if last_cleanup.elapsed() > Duration::from_secs(30) {
                Self::cleanup_fec_sets(&mut fec_set_accumulators);
                last_cleanup = Instant::now();
            }
        }

        Ok(())
    }

    async fn process_fec_shred(
        raw: RawShred,
        fec_set_accumulators: &mut HashMap<(u64, u32), FecSetAccumulator>,
        senders: &Arc<Vec<Sender<CompletedFecSet>>>,
        reed_solomon_cache: &Arc<ReedSolomonCache>,
        processed_fec_sets: &DashSet<(u64, u32)>,
    ) -> Result<()> {
        let slot = raw.slot;
        let fec_set_index = raw.fec_set_index;
        let fec_key = (slot, fec_set_index);

        let accumulator =
            fec_set_accumulators
                .entry(fec_key)
                .or_insert_with(|| FecSetAccumulator {
                    slot,
                    data_shreds: HashMap::new(),
                    code_shreds: HashMap::new(),
                    expected_data_shreds: None,
                    created_at: Instant::now(),
                });

        Self::store_fec_shred(accumulator, raw);
        Self::check_fec_completion(
            fec_key,
            fec_set_accumulators,
            senders,
            reed_solomon_cache,
            processed_fec_sets,
        )
        .await?;

        Ok(())
    }

    fn store_fec_shred(accumulator: &mut FecSetAccumulator, raw: RawShred) {
        let index = raw.index;
        match raw.shred_type {
            ShredType::Code => {
                if accumulator.expected_data_shreds.is_none() {
                    if let Some(n) = raw.num_data_shreds {
                        accumulator.expected_data_shreds = Some(n as usize);
                    }
                }
                accumulator.code_shreds.insert(index, Arc::new(raw));

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_shreds_accumulated
                        .with_label_values(&["code"])
                        .inc();
                }
            }
            ShredType::Data => {
                accumulator.data_shreds.insert(index, Arc::new(raw));

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_shreds_accumulated
                        .with_label_values(&["data"])
                        .inc();
                }
            }
        }
    }

    /// Checks if FEC sets are fully reconstructed and sends them to dispatcher if they are
    async fn check_fec_completion(
        fec_key: (u64, u32),
        fec_set_accumulators: &mut HashMap<(u64, u32), FecSetAccumulator>,
        senders: &Arc<Vec<Sender<CompletedFecSet>>>,
        reed_solomon_cache: &Arc<ReedSolomonCache>,
        processed_fec_sets: &DashSet<(u64, u32)>,
    ) -> Result<()> {
        let acc = if let Some(accumulator) = fec_set_accumulators.get_mut(&fec_key) {
            accumulator
        } else {
            return Ok(());
        };
        let status = Self::can_reconstruct_fec_set(acc);

        match status {
            ReconstructionStatus::ReadyNatural => {
                let acc = fec_set_accumulators.remove(&fec_key).unwrap();
                Self::send_completed_fec_set(acc, senders, fec_key, processed_fec_sets).await?;

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_fec_sets_completed
                        .with_label_values(&["natural"])
                        .inc();
                }
            }
            ReconstructionStatus::ReadyRecovery => {
                let recovery_ok = match Self::recover_fec(acc, reed_solomon_cache) {
                    Ok(()) => true,
                    Err(_e) => {
                        // FEC recovery failed (e.g. InvalidMerkleRoot) — proceed with partial data shreds
                        false
                    }
                };

                let acc = fec_set_accumulators.remove(&fec_key).unwrap();
                Self::send_completed_fec_set(acc, senders, fec_key, processed_fec_sets).await?;

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    let label = if recovery_ok {
                        "recovery"
                    } else {
                        "recovery_partial"
                    };
                    metrics
                        .processor_fec_sets_completed
                        .with_label_values(&[label])
                        .inc();
                }
            }
            ReconstructionStatus::NotReady => {}
        }

        Ok(())
    }

    fn can_reconstruct_fec_set(acc: &FecSetAccumulator) -> ReconstructionStatus {
        let data_count = acc.data_shreds.len();
        let code_count = acc.code_shreds.len();

        if let Some(expected) = acc.expected_data_shreds {
            if data_count >= expected {
                ReconstructionStatus::ReadyNatural
            } else if data_count + code_count >= expected {
                ReconstructionStatus::ReadyRecovery
            } else {
                ReconstructionStatus::NotReady
            }
        } else {
            // We haven't seen a code shred yet, so we don't know the expected count.
            // 32 data shreds is a reasonable heuristic for a full FEC set.
            if data_count >= 32 {
                ReconstructionStatus::ReadyNatural
            } else {
                ReconstructionStatus::NotReady
            }
        }
    }

    /// Reconstructs missing data shreds from code shreds via Reed-Solomon.
    ///
    /// This is the **only** path where we actually construct `Shred` objects
    /// from raw bytes. The natural-completion path skips this entirely.
    fn recover_fec(
        acc: &mut FecSetAccumulator,
        reed_solomon_cache: &Arc<ReedSolomonCache>,
    ) -> Result<()> {
        let mut shreds_for_recovery: Vec<Shred> =
            Vec::with_capacity(acc.data_shreds.len() + acc.code_shreds.len());

        // Parse each raw shred into a full `Shred` only now (lazily).
        // We pass a copy of the bytes since `Shred::new_from_serialized_shred`
        // takes ownership, but we need to keep the Arc<RawShred> around because
        // it is still referenced by the accumulator's maps.
        for raw in acc.data_shreds.values() {
            match Shred::new_from_serialized_shred(raw.bytes.clone()) {
                Ok(s) => shreds_for_recovery.push(s),
                Err(e) => return Err(anyhow::anyhow!("Failed to parse data shred: {:?}", e)),
            }
        }
        for raw in acc.code_shreds.values() {
            match Shred::new_from_serialized_shred(raw.bytes.clone()) {
                Ok(s) => shreds_for_recovery.push(s),
                Err(e) => return Err(anyhow::anyhow!("Failed to parse code shred: {:?}", e)),
            }
        }

        match solana_ledger::shred::recover(shreds_for_recovery, reed_solomon_cache) {
            Ok(recovered_shreds) => {
                for result in recovered_shreds {
                    match result {
                        Ok(recovered_shred) => {
                            if recovered_shred.is_data() {
                                let index = recovered_shred.index();
                                if !acc.data_shreds.contains_key(&index) {
                                    let slot = recovered_shred.slot();
                                    let fec_set_index = recovered_shred.fec_set_index();
                                    let bytes = recovered_shred.payload().clone();
                                    let data_flags = bytes
                                        .get(OFFSET_FLAGS)
                                        .copied()
                                        .unwrap_or(0);
                                    let raw = RawShred {
                                        bytes: bytes.to_vec(),
                                        slot,
                                        index,
                                        fec_set_index,
                                        shred_type: ShredType::Data,
                                        data_flags,
                                        num_data_shreds: None,
                                        received_at_micros: None,
                                    };
                                    acc.data_shreds.insert(index, Arc::new(raw));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow::anyhow!("Failed to recover shred: {:?}", e));
                        }
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("FEC recovery failed: {:?}", e));
            }
        }

        Ok(())
    }

    async fn send_completed_fec_set(
        acc: FecSetAccumulator,
        senders: &Arc<Vec<Sender<CompletedFecSet>>>,
        fec_key: (u64, u32),
        processed_fec_sets: &DashSet<(u64, u32)>,
    ) -> Result<()> {
        let slot = acc.slot;
        let completed_fec_set = CompletedFecSet {
            slot,
            data_shreds: acc.data_shreds,
        };

        // Route by slot so every FEC set of the same slot always lands on the
        // same dispatcher (per-slot state lives on a single task).
        let dispatcher_id = (slot as usize) % senders.len();
        senders[dispatcher_id].send(completed_fec_set).await?;
        processed_fec_sets.insert(fec_key);

        Ok(())
    }

    /// Pulls completed FEC sets, tries to reconstruct batches and dispatch them.
    ///
    /// Each dispatcher shard owns a disjoint subset of slots (`slot % N == id`).
    /// This means per-slot state (`slot_accumulators`) lives entirely on a
    /// single task with no locking.
    async fn dispatch_worker(
        self: Arc<Self>,
        dispatcher_id: usize,
        mut completed_fec_receiver: Receiver<CompletedFecSet>,
        batch_sender: Vec<Sender<BatchWork>>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        let mut slot_accumulators: HashMap<u64, SlotAccumulator> = HashMap::new();
        let mut processed_slots = HashSet::new();
        let mut next_worker = 0usize;
        let mut last_maintenance = Instant::now();
        let mut max_slot_seen: u64 = 0;

        loop {
            match completed_fec_receiver.recv().await {
                Some(completed_fec_set) => {
                    if completed_fec_set.slot > max_slot_seen {
                        max_slot_seen = completed_fec_set.slot;
                    }

                    if let Err(e) = self
                        .accumulate_completed_fec_set(
                            completed_fec_set,
                            &mut slot_accumulators,
                            &processed_slots,
                            &batch_sender,
                            &mut next_worker,
                        )
                        .await
                    {
                        error!("Failed to process completed FEC set: {}", e);
                    }

                    if last_maintenance.elapsed() > std::time::Duration::from_secs(5) {
                        if let Err(e) =
                            Self::cleanup_memory(&mut slot_accumulators, &mut processed_slots)
                        {
                            error!("Could not clean up memory: {:?}", e)
                        }

                        // Only the lowest-id dispatcher prunes the shared dedup
                        // set to avoid redundant work. Others just track the
                        // watermark for their own slot cleanup.
                        if dispatcher_id == 0 {
                            Self::cleanup_processed_fec_sets(&processed_fec_sets, max_slot_seen);
                        }

                        #[cfg(feature = "metrics")]
                        if let Err(e) = Self::update_resource_metrics(&mut slot_accumulators) {
                            error!("Could not update resource metrics: {:?}", e)
                        }

                        last_maintenance = Instant::now();
                    }
                }

                None => {
                    warn!("Dispatcher {}: channel closed", dispatcher_id);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn accumulate_completed_fec_set(
        &self,
        completed_fec_set: CompletedFecSet,
        slot_accumulators: &mut HashMap<u64, SlotAccumulator>,
        processed_slots: &HashSet<u64>,
        batch_senders: &[Sender<BatchWork>],
        next_worker: &mut usize,
    ) -> Result<()> {
        let slot = completed_fec_set.slot;

        if processed_slots.contains(&slot) {
            return Ok(());
        }

        let accumulator = slot_accumulators
            .entry(slot)
            .or_insert_with(|| SlotAccumulator {
                data_shreds: HashMap::new(),
                pending_batch_ends: std::collections::BTreeSet::new(),
                last_processed_batch_idx: None,
                created_at: Instant::now(),
            });

        // Move the data shreds into the slot accumulator. For each shred that
        // carries the batch-end flag, also record it in the sorted pending set
        // so dispatch is O(log n) instead of O(n) scan.
        let last_processed = accumulator.last_processed_batch_idx;
        for (index, raw) in completed_fec_set.data_shreds {
            if (raw.data_flags & 0x40) != 0
                && last_processed.map_or(true, |lp| index > lp)
            {
                accumulator.pending_batch_ends.insert(index);
            }
            accumulator.data_shreds.insert(index, raw);
        }

        self.try_dispatch_complete_batch(accumulator, slot, batch_senders, next_worker)
            .await?;

        Ok(())
    }

    async fn try_dispatch_complete_batch(
        &self,
        accumulator: &mut SlotAccumulator,
        slot: u64,
        batch_senders: &[Sender<BatchWork>],
        next_worker: &mut usize,
    ) -> Result<()> {
        // Drain dispatchable batch-ends in ascending order.
        // Stop at the first gap: later batches can't dispatch until the
        // preceding ones do, because batch_start_idx is derived from
        // last_processed_batch_idx which only advances monotonically.
        loop {
            let batch_end_idx = match accumulator.pending_batch_ends.iter().next().copied() {
                Some(idx) => idx,
                None => break,
            };

            let batch_start_idx = accumulator
                .last_processed_batch_idx
                .map_or(0, |idx| idx + 1);

            let has_all_shreds = (batch_start_idx..=batch_end_idx)
                .all(|i| accumulator.data_shreds.contains_key(&i));
            if !has_all_shreds {
                break; // Wait for missing shreds
            }

            accumulator.pending_batch_ends.remove(&batch_end_idx);

            let mut batch_shreds = HashMap::new();
            for idx in batch_start_idx..=batch_end_idx {
                if let Some(raw) = accumulator.data_shreds.get(&idx) {
                    batch_shreds.insert(idx, Arc::clone(raw));
                }
            }

            let batch_work = BatchWork {
                slot,
                batch_start_idx,
                batch_end_idx,
                shreds: batch_shreds,
            };
            let sender = &batch_senders[*next_worker % batch_senders.len()];
            *next_worker += 1;
            if let Err(e) = sender.send(batch_work).await {
                return Err(anyhow::anyhow!("Failed to send batch work: {}", e));
            }

            accumulator.last_processed_batch_idx = Some(batch_end_idx);
        }

        Ok(())
    }

    async fn batch_worker<H: TransactionHandler>(
        worker_id: usize,
        mut batch_receiver: Receiver<BatchWork>,
        tx_handler: Arc<H>,
    ) -> Result<()> {
        #[cfg(feature = "metrics")]
        let mut last_channel_udpate = std::time::Instant::now();
        while let Some(batch_work) = batch_receiver.recv().await {
            if let Err(e) = Self::process_batch_work(batch_work, &tx_handler).await {
                error!("Batch worker {} failed to process batch: {}", worker_id, e);
            }

            #[cfg(feature = "metrics")]
            if last_channel_udpate.elapsed() > std::time::Duration::from_secs(1) {
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .channel_capacity_utilization
                        .with_label_values(&[&format!("accumulator_batch-worker-{}", worker_id)])
                        .set(
                            (batch_receiver.len() as f64 / batch_receiver.capacity() as f64 * 100.0)
                                as i64,
                        );
                }

                last_channel_udpate = std::time::Instant::now();
            }
        }

        Ok(())
    }

    async fn process_batch_work<H: TransactionHandler>(
        batch_work: BatchWork,
        tx_handler: &Arc<H>,
    ) -> Result<()> {
        let combined_data_meta = Self::get_batch_data(
            &batch_work.shreds,
            batch_work.batch_start_idx,
            batch_work.batch_end_idx,
        )?;

        let entries = Self::parse_entries_from_batch_data(combined_data_meta)?;

        for entry_meta in entries {
            Self::process_entry_transactions(batch_work.slot, &entry_meta, tx_handler).await?;
        }

        Ok(())
    }

    fn get_batch_data(
        shreds: &HashMap<u32, Arc<RawShred>>,
        start_idx: u32,
        end_idx: u32,
    ) -> Result<CombinedDataMeta> {
        let size: usize = (end_idx - start_idx + 1) as usize;
        // Pre-compute total data length to do exactly one allocation.
        let mut total_data_len = 0usize;
        for idx in start_idx..=end_idx {
            let raw = shreds
                .get(&idx)
                .ok_or_else(|| anyhow::anyhow!("Missing shred at index {}", idx))?;
            let payload = &raw.bytes;
            if payload.len() < OFFSET_SIZE + 2 {
                return Err(anyhow::anyhow!("Invalid payload"));
            }
            let total_size =
                u16::from_le_bytes([payload[OFFSET_SIZE], payload[OFFSET_SIZE + 1]]) as usize;
            let data_size = total_size.saturating_sub(DATA_OFFSET_PAYLOAD);
            total_data_len += data_size;
        }

        let mut combined_data = Vec::with_capacity(total_data_len);
        let mut combined_data_shred_indices = Vec::with_capacity(size);
        let mut combined_data_shred_received_at_micros = Vec::with_capacity(size);

        for idx in start_idx..=end_idx {
            let raw = shreds.get(&idx).unwrap(); // checked above
            let payload = &raw.bytes;
            let total_size =
                u16::from_le_bytes([payload[OFFSET_SIZE], payload[OFFSET_SIZE + 1]]) as usize;
            let data_size = total_size.saturating_sub(DATA_OFFSET_PAYLOAD);

            combined_data_shred_received_at_micros.push(raw.received_at_micros);
            combined_data_shred_indices.push(combined_data.len());

            if let Some(data) = payload.get(DATA_OFFSET_PAYLOAD..DATA_OFFSET_PAYLOAD + data_size) {
                combined_data.extend_from_slice(data);
            } else {
                return Err(anyhow::anyhow!("Missing data in shred"));
            }
        }

        Ok(CombinedDataMeta {
            combined_data,
            combined_data_shred_indices,
            combined_data_shred_received_at_micros,
        })
    }

    fn parse_entries_from_batch_data(
        combined_data_meta: CombinedDataMeta,
    ) -> Result<Vec<EntryMeta>> {
        let combined_data = combined_data_meta.combined_data;
        if combined_data.len() <= 8 {
            return Ok(Vec::new());
        }
        let shred_indices = &combined_data_meta.combined_data_shred_indices;
        let shred_received_at_micros = &combined_data_meta.combined_data_shred_received_at_micros;

        let entry_count = u64::from_le_bytes(combined_data[0..8].try_into()?);
        let mut cursor = Cursor::new(&combined_data);
        cursor.set_position(8);

        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let entry_start_pos = cursor.position() as usize;

            match bincode::deserialize_from::<_, Entry>(&mut cursor) {
                Ok(entry) => {
                    let earliest_timestamp = Self::find_earliest_contributing_shred_timestamp(
                        entry_start_pos,
                        shred_indices,
                        shred_received_at_micros,
                    )?;

                    entries.push(EntryMeta {
                        entry,
                        received_at_micros: earliest_timestamp,
                    });
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error deserializing entry {:?}", e));
                }
            }
        }

        Ok(entries)
    }

    fn find_earliest_contributing_shred_timestamp(
        entry_start_pos: usize,
        shred_indices: &[usize],
        shred_received_at_micros: &[Option<u64>],
    ) -> Result<Option<u64>> {
        let shred_idx = match shred_indices.binary_search(&entry_start_pos) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        };

        Ok(shred_received_at_micros.get(shred_idx).and_then(|&ts| ts))
    }

    async fn process_entry_transactions<H: TransactionHandler>(
        slot: u64,
        entry_meta: &EntryMeta,
        handler: &Arc<H>,
    ) -> Result<()> {
        for tx in &entry_meta.entry.transactions {
            let event = TransactionEvent {
                slot,
                transaction: tx,
                received_at_micros: entry_meta.received_at_micros,
                processed_at_micros: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
            };

            if let Err(e) = handler.handle_transaction(&event) {
                error!("Transaction handler error: {:?}", e);
                continue;
            }

            #[cfg(feature = "metrics")]
            {
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_transactions_processed
                        .with_label_values(&["all"])
                        .inc();
                }

                if let Some(received_at) = event.received_at_micros {
                    let received_at_unix = UNIX_EPOCH + Duration::from_micros(received_at);
                    let processed_at_unix =
                        UNIX_EPOCH + Duration::from_micros(event.processed_at_micros);

                    if let Ok(processing_latency) =
                        processed_at_unix.duration_since(received_at_unix)
                    {
                        if let Some(metrics) = Metrics::try_get() {
                            metrics
                                .processing_latency
                                .with_label_values(&["transaction"])
                                .observe(processing_latency.as_secs_f64());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn cleanup_fec_sets(fec_sets: &mut HashMap<(u64, u32), FecSetAccumulator>) {
        let now = Instant::now();
        // Shreds either arrive within a second on mainnet or they never arrive.
        // 5s is a generous ceiling that bounds partial-FEC memory usage.
        let max_age = Duration::from_secs(5);
        fec_sets.retain(|_, acc| now.duration_since(acc.created_at) <= max_age);
    }

    /// Drops entries from the processed-FEC-set dedup table whose slot is
    /// more than `SLOT_RETAIN_WINDOW` slots behind the newest one we've seen.
    ///
    /// Without this the set grows unbounded for the lifetime of the process.
    fn cleanup_processed_fec_sets(
        processed_fec_sets: &DashSet<(u64, u32)>,
        max_slot_seen: u64,
    ) {
        // ~400ms/slot × 500 ≈ 200s window. Deduping beyond that is pointless —
        // no turbine peer will still be replaying shreds from that far back.
        const SLOT_RETAIN_WINDOW: u64 = 500;
        if max_slot_seen <= SLOT_RETAIN_WINDOW {
            return;
        }
        let cutoff = max_slot_seen - SLOT_RETAIN_WINDOW;
        processed_fec_sets.retain(|&(slot, _)| slot >= cutoff);
    }

    pub fn cleanup_memory(
        slot_accumulators: &mut HashMap<u64, SlotAccumulator>,
        processed_slots: &mut HashSet<u64>,
    ) -> Result<()> {
        let now = Instant::now();
        // 10s is enough headroom for late-arriving FEC sets from slow peers
        // while still bounding memory if a single shred in a batch goes missing.
        let max_age = Duration::from_secs(10);
        let slots_to_remove: Vec<u64> = slot_accumulators
            .iter()
            .filter_map(|(slot, acc)| {
                if now.duration_since(acc.created_at) > max_age {
                    Some(*slot)
                } else {
                    None
                }
            })
            .collect();

        for slot in slots_to_remove {
            slot_accumulators.remove(&slot);
            processed_slots.remove(&slot);
        }

        Ok(())
    }

    #[cfg(feature = "metrics")]
    pub fn update_resource_metrics(
        slot_accumulators: &mut HashMap<u64, SlotAccumulator>,
    ) -> Result<()> {
        let unique_slots: HashSet<u64> = slot_accumulators.keys().map(|slot| *slot).collect();
        if let Some(metrics) = Metrics::try_get() {
            metrics
                .active_slots
                .with_label_values(&["accumulation"])
                .set(unique_slots.len() as i64);
        }

        Ok(())
    }
}
