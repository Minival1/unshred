#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::{types::ShredBytesMeta, TransactionEvent, TransactionHandler, UnshredConfig};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::Result;
use dashmap::DashSet;
use solana_entry::entry::Entry;
use solana_ledger::shred::{ReedSolomonCache, Shred, ShredType};
use std::{
    io::Cursor,
    time::{Instant, SystemTime, UNIX_EPOCH},
    u64,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

// Header offsets
const OFFSET_FLAGS: usize = 85;
const OFFSET_SIZE: usize = 86; // Payload total size offset
const DATA_OFFSET_PAYLOAD: usize = 88;

#[derive(Debug, Clone)]
pub struct CompletedFecSet {
    pub slot: u64,
    pub data_shreds: HashMap<u32, ShredMeta>,
}

struct FecSetAccumulator {
    slot: u64,
    data_shreds: HashMap<u32, ShredMeta>,
    code_shreds: HashMap<u32, ShredMeta>,
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
    pub shreds: HashMap<u32, ShredMeta>,
}

struct CombinedDataMeta {
    combined_data_shred_indices: Vec<usize>,
    combined_data_shred_received_at_micros: Vec<Option<u64>>,
    combined_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ShredMeta {
    pub shred: Shred,
    pub received_at_micros: Option<u64>,
}

#[derive(Debug)]
pub struct EntryMeta {
    pub entry: Entry,
    pub received_at_micros: Option<u64>,
}

pub struct SlotAccumulator {
    data_shreds: HashMap<u32, ShredMeta>, // index -> shred
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
        // Channel for fec workers -> batch dispatcher worker
        let (completed_fec_sender, completed_fec_receiver) =
            tokio::sync::mpsc::channel::<CompletedFecSet>(1000);

        // Track processed fec sets for  deduplication
        let processed_fec_sets = Arc::new(DashSet::<(u64, u32)>::new());

        // Channels for receiver -> fec workers
        let num_fec_workers = match config.num_fec_workers {
            Some(num) => num,
            None => total_cores.saturating_sub(2) as u8,
        };
        let num_fec_workers = std::cmp::max(num_fec_workers, 1);
        let (shred_senders, shred_receivers): (Vec<_>, Vec<_>) = (0..num_fec_workers)
            .map(|_| tokio::sync::mpsc::channel::<ShredBytesMeta>(10000))
            .unzip();

        // Spawn network receiver
        let bind_addr: std::net::SocketAddr = config.bind_address.parse()?;
        let receiver = crate::receiver::ShredReceiver::new(bind_addr)?;
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
            let sender = completed_fec_sender.clone();
            let processed_fec_sets_clone = Arc::clone(&processed_fec_sets);

            let handle = tokio::spawn(async move {
                if let Err(e) =
                    Self::run_fec_worker(worker_id, fec_receiver, sender, processed_fec_sets_clone)
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

        // Spawn batch dispatch worker
        let processor = Arc::new(self);
        let dispatch_handle = {
            let senders = batch_senders.clone();
            let proc = Arc::clone(&processor);

            tokio::spawn(async move {
                if let Err(e) = proc.dispatch_worker(completed_fec_receiver, senders).await {
                    error!("Accumulation worker failed: {:?}", e)
                }
            })
        };

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
        dispatch_handle.await?;
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
        mut receiver: Receiver<ShredBytesMeta>,
        sender: Sender<CompletedFecSet>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        let reed_solomon_cache = Arc::new(ReedSolomonCache::default());
        let mut fec_set_accumulators: HashMap<(u64, u32), FecSetAccumulator> = HashMap::new();
        let mut last_cleanup = Instant::now();
        #[cfg(feature = "metrics")]
        let mut last_channel_udpate = Instant::now();

        loop {
            match receiver.recv().await {
                Some(shred_bytes_meta) => {
                    if let Err(e) = Self::process_fec_shred(
                        shred_bytes_meta,
                        &mut fec_set_accumulators,
                        &sender,
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
        shred_bytes_meta: ShredBytesMeta,
        fec_set_accumulators: &mut HashMap<(u64, u32), FecSetAccumulator>,
        sender: &Sender<CompletedFecSet>,
        reed_solomon_cache: &Arc<ReedSolomonCache>,
        processed_fec_sets: &DashSet<(u64, u32)>,
    ) -> Result<()> {
        let shred = match Shred::new_from_serialized_shred(shred_bytes_meta.shred_bytes.to_vec()) {
            Ok(shred) => shred,
            Err(e) => {
                error!("Failed to parse shred: {}", e);
                return Ok(());
            }
        };

        let slot = shred.slot();
        let fec_set_index = shred.fec_set_index();
        let fec_key = (slot, fec_set_index);

        let accumulator =
            fec_set_accumulators
                .entry(fec_key)
                .or_insert_with(|| FecSetAccumulator {
                    slot: slot,
                    data_shreds: HashMap::new(),
                    code_shreds: HashMap::new(),
                    expected_data_shreds: None,
                    created_at: Instant::now(),
                });

        let shred_meta = ShredMeta {
            shred,
            received_at_micros: shred_bytes_meta.received_at_micros,
        };

        Self::store_fec_shred(accumulator, shred_meta)?;
        Self::check_fec_completion(
            fec_key,
            fec_set_accumulators,
            sender,
            reed_solomon_cache,
            processed_fec_sets,
        )
        .await?;

        Ok(())
    }

    fn store_fec_shred(accumulator: &mut FecSetAccumulator, shred_meta: ShredMeta) -> Result<()> {
        match shred_meta.shred.shred_type() {
            ShredType::Code => {
                let payload = shred_meta.shred.payload();
                if accumulator.expected_data_shreds.is_none() && payload.len() >= 85 {
                    let expected = u16::from_le_bytes([payload[83], payload[84]]) as usize;
                    accumulator.expected_data_shreds = Some(expected);
                }

                accumulator
                    .code_shreds
                    .insert(shred_meta.shred.index(), shred_meta);

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_shreds_accumulated
                        .with_label_values(&["code"])
                        .inc();
                }
            }
            ShredType::Data => {
                accumulator
                    .data_shreds
                    .insert(shred_meta.shred.index(), shred_meta);

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_shreds_accumulated
                        .with_label_values(&["data"])
                        .inc();
                }
            }
        }

        Ok(())
    }

    /// Checks if FEC sets are fully reconstructed and sends them to dispatcher if they are
    async fn check_fec_completion(
        fec_key: (u64, u32),
        fec_set_accumulators: &mut HashMap<(u64, u32), FecSetAccumulator>,
        sender: &Sender<CompletedFecSet>,
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
                Self::send_completed_fec_set(acc, sender, fec_key, processed_fec_sets).await?;

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_fec_sets_completed
                        .with_label_values(&["natural"])
                        .inc();
                }
            }
            ReconstructionStatus::ReadyRecovery => {
                let recovery_ok = match Self::recover_fec(acc, reed_solomon_cache).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!("FEC Recovery failed (using partial data shreds): {:?}", e);
                        false
                    }
                };

                let acc = fec_set_accumulators.remove(&fec_key).unwrap();
                Self::send_completed_fec_set(acc, sender, fec_key, processed_fec_sets).await?;

                #[cfg(feature = "metrics")]
                if let Some(metrics) = Metrics::try_get() {
                    let label = if recovery_ok { "recovery" } else { "recovery_partial" };
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
                // Priority
                ReconstructionStatus::ReadyNatural
            } else if data_count + code_count >= expected {
                ReconstructionStatus::ReadyRecovery
            } else {
                ReconstructionStatus::NotReady
            }
        } else {
            // Minor case optimization: we don't have `expected`
            // because we haven't seen a code shred yet.
            // Assume having 32 data shreds is enough.
            if data_count >= 32 {
                ReconstructionStatus::ReadyNatural
            } else {
                ReconstructionStatus::NotReady
            }
        }
    }

    async fn recover_fec(
        acc: &mut FecSetAccumulator,
        reed_solomon_cache: &Arc<ReedSolomonCache>,
    ) -> Result<()> {
        let mut shreds_for_recovery = Vec::new();

        for (_, shred_meta) in &acc.data_shreds {
            shreds_for_recovery.push(shred_meta.shred.clone());
        }

        for (_, shred_meta) in &acc.code_shreds {
            shreds_for_recovery.push(shred_meta.shred.clone());
        }

        match solana_ledger::shred::recover(shreds_for_recovery, reed_solomon_cache) {
            Ok(recovered_shreds) => {
                for result in recovered_shreds {
                    match result {
                        Ok(recovered_shred) => {
                            if recovered_shred.is_data() {
                                let index = recovered_shred.index();
                                if !acc.data_shreds.contains_key(&index) {
                                    acc.data_shreds.insert(
                                        index,
                                        ShredMeta {
                                            shred: recovered_shred,
                                            received_at_micros: None,
                                        },
                                    );
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
        sender: &Sender<CompletedFecSet>,
        fec_key: (u64, u32),
        processed_fec_sets: &DashSet<(u64, u32)>,
    ) -> Result<()> {
        let completed_fec_set = CompletedFecSet {
            slot: acc.slot,
            data_shreds: acc.data_shreds,
        };

        sender.send(completed_fec_set).await?;
        processed_fec_sets.insert(fec_key);

        Ok(())
    }

    /// Pulls completed FEC sets, tries to reconstruct batches and dispatch them
    async fn dispatch_worker(
        self: Arc<Self>,
        mut completed_fec_receiver: Receiver<CompletedFecSet>,
        batch_sender: Vec<Sender<BatchWork>>,
    ) -> Result<()> {
        let mut slot_accumulators: HashMap<u64, SlotAccumulator> = HashMap::new();
        let mut processed_slots = HashSet::new();
        let mut next_worker = 0usize;
        let mut last_maintenance = Instant::now();

        loop {
            match completed_fec_receiver.recv().await {
                Some(completed_fec_set) => {
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

                    if last_maintenance.elapsed() > std::time::Duration::from_secs(1) {
                        // Clean up
                        if let Err(e) =
                            Self::cleanup_memory(&mut slot_accumulators, &mut processed_slots)
                        {
                            error!("Could not clean up memory: {:?}", e)
                        }

                        // Metrics
                        #[cfg(feature = "metrics")]
                        if let Err(e) = Self::update_resource_metrics(&mut slot_accumulators) {
                            error!("Could not update resource metrics: {:?}", e)
                        }

                        last_maintenance = Instant::now();
                    }
                }

                None => {
                    warn!("FEC accumulation worker: Channel closed");
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
                last_processed_batch_idx: None,
                created_at: Instant::now(),
            });

        // Add all data shreds from completed FEC set
        for (index, shred_meta) in completed_fec_set.data_shreds {
            accumulator.data_shreds.insert(index, shred_meta);
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
        let last_processed = accumulator.last_processed_batch_idx.unwrap_or(0);

        // Find any new batch complete indices
        let mut new_batch_complete_indices: Vec<u32> = accumulator
            .data_shreds
            .iter()
            .filter_map(|(idx, shred_meta)| {
                if *idx <= last_processed {
                    return None;
                }

                let payload = shred_meta.shred.payload();
                if let Some(data_flags) = payload.get(OFFSET_FLAGS) {
                    if (data_flags & 0x40) != 0 {
                        Some(*idx)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        new_batch_complete_indices.sort_unstable();

        // Dispatch completed batches
        for batch_end_idx in new_batch_complete_indices {
            let batch_start_idx = accumulator
                .last_processed_batch_idx
                .map_or(0, |idx| idx + 1);

            let has_all_shreds =
                (batch_start_idx..=batch_end_idx).all(|i| accumulator.data_shreds.contains_key(&i));

            if !has_all_shreds {
                continue; // Wait for missing shreds
            }

            // Get batch shreds
            let mut batch_shreds = HashMap::new();
            for idx in batch_start_idx..=batch_end_idx {
                if let Some(shred_meta) = accumulator.data_shreds.get(&idx) {
                    batch_shreds.insert(idx, shred_meta.clone());
                }
            }

            // Send
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

            // Update metrics periodically
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
        shreds: &HashMap<u32, ShredMeta>,
        start_idx: u32,
        end_idx: u32,
    ) -> Result<CombinedDataMeta> {
        // Track what bytes were contributed by what shreds (for timing stats)
        let mut combined_data = Vec::new();
        let size: usize = (end_idx - start_idx) as usize;
        let mut combined_data_shred_indices = Vec::with_capacity(size);
        let mut combined_data_shred_received_at_micros = Vec::with_capacity(size);

        // Go through shreds in order
        for idx in start_idx..=end_idx {
            let shred_meta = shreds
                .get(&idx)
                .ok_or_else(|| anyhow::anyhow!("Missing shred at index {}", idx))?;

            combined_data_shred_received_at_micros.push(shred_meta.received_at_micros);
            combined_data_shred_indices.push(combined_data.len());

            let payload = shred_meta.shred.payload();
            if payload.len() >= OFFSET_SIZE + 2 {
                let size_bytes = &payload[OFFSET_SIZE..OFFSET_SIZE + 2];
                let total_size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
                let data_size = total_size.saturating_sub(DATA_OFFSET_PAYLOAD);

                if let Some(data) =
                    payload.get(DATA_OFFSET_PAYLOAD..DATA_OFFSET_PAYLOAD + data_size)
                {
                    combined_data.extend_from_slice(data);
                } else {
                    return Err(anyhow::anyhow!("Missing data in shred"));
                }
            } else {
                return Err(anyhow::anyhow!("Invalid payload"));
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
            Ok(idx) => idx, // Entry starts exactly at start of shred
            Err(idx) => {
                idx - 1 // Entry starts in the middle of the previous shred
            }
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

            // Count total transactions of any type found
            #[cfg(feature = "metrics")]
            {
                if let Some(metrics) = Metrics::try_get() {
                    metrics
                        .processor_transactions_processed
                        .with_label_values(&["all"])
                        .inc();
                }

                // Calculate latency from shred to tx
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
        let max_age = Duration::from_secs(30);
        fec_sets.retain(|_, acc| now.duration_since(acc.created_at) <= max_age);
    }

    pub fn cleanup_memory(
        slot_accumulators: &mut HashMap<u64, SlotAccumulator>,
        processed_slots: &mut HashSet<u64>,
    ) -> Result<()> {
        let now = Instant::now();
        // Remove old slots from memory
        // We aren't expecting to get any more shreds for them
        let max_age = Duration::from_secs(30);
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
