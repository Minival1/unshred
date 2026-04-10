#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::types::RawShred;

use anyhow::Result;
use dashmap::DashSet;
use socket2::{Domain, Socket, Type};
use solana_ledger::shred::ShredType;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc::Sender, task};
use tracing::{error, info};

const SHRED_SIZE: usize = 1228;
const RECV_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64MB
const RECV_BATCH: usize = 64;

// Header layout offsets (common shred header)
const OFFSET_VARIANT: usize = 64;
const OFFSET_SHRED_SLOT: usize = 65;
const OFFSET_SHRED_INDEX: usize = 73;
const OFFSET_FEC_SET_INDEX: usize = 79;
// Data-shred fields
const OFFSET_FLAGS: usize = 85;
// Coding-shred fields (num_data_shreds at 83..85)
const OFFSET_CODE_NUM_DATA: usize = 83;

const MIN_HEADER_LEN: usize = 88;

pub struct ShredReceiver {
    sockets: Vec<Arc<Socket>>,
}

impl ShredReceiver {
    pub fn new(bind_addr: SocketAddr, num_receivers: usize) -> Result<Self> {
        let num_receivers = num_receivers.max(1);
        let mut sockets = Vec::with_capacity(num_receivers);

        for i in 0..num_receivers {
            let socket = Self::build_socket(bind_addr, num_receivers > 1)?;
            info!("UDP receiver #{} bound to {}", i, bind_addr);
            sockets.push(Arc::new(socket));
        }

        Ok(Self { sockets })
    }

    fn build_socket(bind_addr: SocketAddr, reuse_port: bool) -> Result<Socket> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;

        socket.set_reuse_address(true)?;
        // SO_REUSEPORT is required for kernel-side flow-hash load balancing
        // across multiple sockets bound to the same (addr, port).
        #[cfg(unix)]
        socket.set_reuse_port(true)?;
        let _ = reuse_port;

        socket.set_recv_buffer_size(RECV_BUFFER_SIZE)?;

        // Busy poll (Linux only).
        #[cfg(target_os = "linux")]
        {
            use libc::{setsockopt, SOL_SOCKET, SO_BUSY_POLL};
            unsafe {
                let busy_poll: libc::c_int = 50; // microseconds
                use std::os::unix::io::AsRawFd;
                let result = setsockopt(
                    socket.as_raw_fd(),
                    SOL_SOCKET,
                    SO_BUSY_POLL,
                    &busy_poll as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&busy_poll) as libc::socklen_t,
                );
                if result < 0 {
                    tracing::warn!("Failed to set SO_BUSY_POLL");
                }
            }
        }

        // Blocking reads — recvmmsg on Linux, single recv elsewhere.
        socket.set_nonblocking(false)?;
        socket.bind(&bind_addr.into())?;
        Ok(socket)
    }

    pub async fn run(
        self,
        senders: Vec<Sender<RawShred>>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        let num_receivers = self.sockets.len();
        info!("Starting {} network receiver workers", num_receivers);
        let mut handles = Vec::with_capacity(num_receivers);

        for (i, socket) in self.sockets.into_iter().enumerate() {
            let senders = senders.clone();
            let processed_fec_sets = Arc::clone(&processed_fec_sets);

            let handle = task::spawn_blocking(move || {
                if let Err(e) = Self::receive_loop(socket, senders, processed_fec_sets) {
                    error!("Receiver {} failed: {}", i, e);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    fn receive_loop(
        socket: Arc<Socket>,
        senders: Vec<Sender<RawShred>>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        #[cfg(feature = "metrics")]
        let mut last_channel_update = std::time::Instant::now();

        let num_senders = senders.len();

        // Pre-allocated batch buffers reused across iterations.
        let mut batch_buffers: Vec<[u8; SHRED_SIZE]> = vec![[0u8; SHRED_SIZE]; RECV_BATCH];
        let mut batch_sizes: [usize; RECV_BATCH] = [0; RECV_BATCH];

        loop {
            let n = match Self::recv_batch(&socket, &mut batch_buffers, &mut batch_sizes) {
                Ok(n) => n,
                Err(e) => {
                    error!("Socket receive error: {}", e);
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = Metrics::try_get() {
                        metrics
                            .errors
                            .with_label_values(&["receiver", "socket_receive"])
                            .inc();
                    }
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
            };

            if n == 0 {
                continue;
            }

            let received_at_micros = now_micros_coarse();

            for i in 0..n {
                let size = batch_sizes[i];
                if size == 0 {
                    continue;
                }
                let data = &batch_buffers[i][..size];

                if let Err(e) = Self::process_shred(
                    data,
                    &senders,
                    num_senders,
                    &processed_fec_sets,
                    received_at_micros,
                ) {
                    // Invalid shreds are expected (garbage packets), don't log-spam.
                    let _ = e;
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = Metrics::try_get() {
                        metrics
                            .errors
                            .with_label_values(&["receiver", "process_shred"])
                            .inc();
                    }
                }
            }

            #[cfg(feature = "metrics")]
            if last_channel_update.elapsed() > Duration::from_secs(1) {
                if let Ok((buf_used, buf_size)) = Self::get_socket_buffer_stats(&socket) {
                    if buf_size > 0 {
                        let utilization = (buf_used as f64 / buf_size as f64) * 100.0;
                        if let Some(metrics) = Metrics::try_get() {
                            metrics
                                .receiver_socket_buffer_utilization
                                .with_label_values(&["receiver"])
                                .set(utilization as i64)
                        }
                    }
                }
                last_channel_update = std::time::Instant::now();
            }
        }
    }

    /// Batched UDP receive.
    ///
    /// On Linux this uses `recvmmsg(2)` for up to `RECV_BATCH` packets in a
    /// single syscall. On other platforms we fall back to a single blocking
    /// `recv` — still correct, just slower.
    #[cfg(target_os = "linux")]
    fn recv_batch(
        socket: &Socket,
        buffers: &mut [[u8; SHRED_SIZE]],
        sizes: &mut [usize],
    ) -> std::io::Result<usize> {
        use std::os::unix::io::AsRawFd;

        let fd = socket.as_raw_fd();
        let batch = buffers.len().min(sizes.len()).min(RECV_BATCH);

        let mut iovecs: [libc::iovec; RECV_BATCH] = unsafe { std::mem::zeroed() };
        let mut mmsghdrs: [libc::mmsghdr; RECV_BATCH] = unsafe { std::mem::zeroed() };

        for i in 0..batch {
            iovecs[i].iov_base = buffers[i].as_mut_ptr() as *mut libc::c_void;
            iovecs[i].iov_len = SHRED_SIZE;
            mmsghdrs[i].msg_hdr.msg_iov = &mut iovecs[i];
            mmsghdrs[i].msg_hdr.msg_iovlen = 1;
        }

        // Blocking call: returns as soon as at least one packet is available,
        // then drains up to `batch` packets that are already queued.
        let ret = unsafe {
            libc::recvmmsg(
                fd,
                mmsghdrs.as_mut_ptr(),
                batch as libc::c_uint,
                0,
                std::ptr::null_mut(),
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                return Ok(0);
            }
            return Err(err);
        }

        let n = ret as usize;
        for i in 0..n {
            sizes[i] = mmsghdrs[i].msg_len as usize;
        }
        Ok(n)
    }

    #[cfg(not(target_os = "linux"))]
    fn recv_batch(
        socket: &Socket,
        buffers: &mut [[u8; SHRED_SIZE]],
        sizes: &mut [usize],
    ) -> std::io::Result<usize> {
        use std::mem::MaybeUninit;
        // Fallback: one packet per syscall.
        let buf = &mut buffers[0];
        // SAFETY: [u8; N] and [MaybeUninit<u8>; N] have the same layout.
        let uninit: &mut [MaybeUninit<u8>] = unsafe {
            std::slice::from_raw_parts_mut(
                buf.as_mut_ptr() as *mut MaybeUninit<u8>,
                buf.len(),
            )
        };
        match socket.recv(uninit) {
            Ok(size) => {
                sizes[0] = size;
                Ok(if size > 0 { 1 } else { 0 })
            }
            Err(e) => Err(e),
        }
    }

    /// Parses header fields from the raw packet and dispatches to a FEC worker.
    fn process_shred(
        buffer: &[u8],
        senders: &[Sender<RawShred>],
        num_senders: usize,
        processed_fec_sets: &DashSet<(u64, u32)>,
        received_at_micros: u64,
    ) -> Result<()> {
        if buffer.len() < MIN_HEADER_LEN {
            return Err(anyhow::anyhow!("Invalid shred size"));
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = Metrics::try_get() {
            metrics
                .receiver_shreds_received
                .with_label_values(&["raw"])
                .inc();
        }

        // Parse header fields from fixed offsets — cheaper than full Shred parse.
        let variant = buffer[OFFSET_VARIANT];
        let shred_type = if (variant & 0x80) != 0 {
            ShredType::Data
        } else {
            ShredType::Code
        };

        let slot = u64::from_le_bytes(
            buffer[OFFSET_SHRED_SLOT..OFFSET_SHRED_SLOT + 8]
                .try_into()
                .unwrap(),
        );
        let index = u32::from_le_bytes(
            buffer[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + 4]
                .try_into()
                .unwrap(),
        );
        let fec_set_index = u32::from_le_bytes(
            buffer[OFFSET_FEC_SET_INDEX..OFFSET_FEC_SET_INDEX + 4]
                .try_into()
                .unwrap(),
        );

        let fec_key = (slot, fec_set_index);
        if processed_fec_sets.contains(&fec_key) {
            return Ok(());
        }

        let (data_flags, num_data_shreds) = match shred_type {
            ShredType::Data => (buffer[OFFSET_FLAGS], None),
            ShredType::Code => {
                let n = u16::from_le_bytes([
                    buffer[OFFSET_CODE_NUM_DATA],
                    buffer[OFFSET_CODE_NUM_DATA + 1],
                ]);
                (0u8, Some(n))
            }
        };

        // Allocate exactly once: own the packet bytes in a tight Vec.
        let raw = RawShred {
            bytes: buffer.to_vec(),
            slot,
            index,
            fec_set_index,
            shred_type,
            data_flags,
            num_data_shreds,
            received_at_micros: Some(received_at_micros),
        };

        // Shard by fec_set_index to keep per-FEC-set state thread-local.
        //
        // Solana's `fec_set_index` is always a multiple of 32 (one FEC set =
        // 32 data shreds), so a naive `fec_set_index % num_senders` with
        // num_senders ∈ {2, 4, 8, 16, 32} collapses to the same worker for
        // every shred. Divide by 32 first so consecutive FEC sets round-robin
        // across workers.
        let worker_id = ((fec_set_index as usize) / 32) % num_senders;
        match senders[worker_id].try_send(raw) {
            Ok(_) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(anyhow::anyhow!("Channel full, backpressure detected"))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("Channel disconnected"))
            }
        }
    }

    #[cfg(feature = "metrics")]
    #[cfg(target_os = "linux")]
    fn get_socket_buffer_stats(socket: &Socket) -> Result<(usize, usize)> {
        use std::os::unix::io::AsRawFd;

        let fd = socket.as_raw_fd();
        let mut recv_buf_used = 0i32;
        let mut recv_buf_size = 0i32;
        let mut len = std::mem::size_of::<i32>() as libc::socklen_t;

        unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &mut recv_buf_size as *mut _ as *mut libc::c_void,
                &mut len,
            );
            libc::ioctl(fd, libc::FIONREAD, &mut recv_buf_used);
        }

        Ok((recv_buf_used as usize, recv_buf_size as usize))
    }

    #[cfg(feature = "metrics")]
    #[cfg(not(target_os = "linux"))]
    fn get_socket_buffer_stats(_socket: &Socket) -> Result<(usize, usize)> {
        Ok((0, 0))
    }
}

/// Wall-clock in microseconds using the precise `CLOCK_REALTIME` via vDSO.
///
/// We previously used `CLOCK_REALTIME_COARSE` but its jiffy-bound granularity
/// (1–4 ms depending on HZ) added quantization noise to per-shred timestamps
/// and visibly inflated the measured `processing_latency` percentiles.
#[inline]
fn now_micros_coarse() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
