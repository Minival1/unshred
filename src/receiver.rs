#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::types::ShredBytesMeta;

use anyhow::Result;
use dashmap::DashSet;
use socket2::{Domain, Socket, Type};
use std::{
    mem::MaybeUninit,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc::Sender, task};
use tracing::{error, info};

const SHRED_SIZE: usize = 1228;
const RECV_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64MB
const OFFSET_SHRED_SLOT: usize = 65;
const OFFSET_FEC_SET_INDEX: usize = 79;

pub struct ShredReceiver {
    socket: Arc<Socket>,
}

impl ShredReceiver {
    pub fn new(bind_addr: SocketAddr) -> Result<Self> {
        // UDP socket
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;

        socket.set_reuse_address(true)?;
        socket.set_recv_buffer_size(RECV_BUFFER_SIZE)?;
        socket.set_nonblocking(true)?;

        // Busy poll
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

        socket.bind(&bind_addr.into())?;
        info!("UDP receiver bound to {}", bind_addr);

        Ok(Self {
            socket: Arc::new(socket),
        })
    }

    pub async fn run(
        self,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        // Spawn receiver threads
        let num_receivers = 1;
        info!("Starting {} network receiver workers", num_receivers);
        let mut handles = Vec::with_capacity(num_receivers);

        for i in 0..num_receivers {
            let socket = Arc::clone(&self.socket);
            let senders = senders.clone();
            let processed_fec_sets = Arc::clone(&processed_fec_sets);

            let handle = task::spawn_blocking(move || {
                if let Err(e) = Self::receive_loop(socket, senders, processed_fec_sets) {
                    error!("Reciever {} failed: {}", i, e);
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
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        #[cfg(feature = "metrics")]
        let mut last_channel_update = std::time::Instant::now();
        // Pre-allocate buffer
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); SHRED_SIZE];

        loop {
            match socket.recv(&mut buffer) {
                Ok(size) if size > 0 => {
                    let received_at_micros = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64;

                    // SAFETY: socket.recv() guarantees the first `size` bytes are initialized
                    let initialized_data =
                        unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, size) };

                    if let Err(e) = Self::process_shred(
                        initialized_data,
                        &senders,
                        &processed_fec_sets,
                        &received_at_micros,
                    ) {
                        error!("Receiver failed to process shred: {}", e);
                        #[cfg(feature = "metrics")]
                        if let Some(metrics) = Metrics::try_get() {
                            metrics
                                .errors
                                .with_label_values(&["receiver", "process_shred"])
                                .inc();
                        }
                    }
                }
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
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
                }
            }

            // Update metrics periodically
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

    /// Creates ShredBytesMeta and sends through `senders`
    fn process_shred(
        buffer: &[u8],
        senders: &[Sender<ShredBytesMeta>],
        processed_fec_sets: &DashSet<(u64, u32)>,
        received_at_micros: &u64,
    ) -> Result<()> {
        if buffer.len() < 88 {
            // Minimum shred header size
            return Err(anyhow::anyhow!("Invalid shred size"));
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = Metrics::try_get() {
            metrics
                .receiver_shreds_received
                .with_label_values(&["raw"])
                .inc();
        }

        // Parse shred header
        let slot = u64::from_le_bytes(buffer[OFFSET_SHRED_SLOT..OFFSET_SHRED_SLOT + 8].try_into()?);
        let fec_set_index =
            u32::from_le_bytes(buffer[OFFSET_FEC_SET_INDEX..OFFSET_FEC_SET_INDEX + 4].try_into()?);

        let fec_key = (slot, fec_set_index);
        if processed_fec_sets.contains(&fec_key) {
            return Ok(()); // Exit early
        }

        // Send ShredBytesMeta to processor
        let worker_id = (fec_set_index as usize) % senders.len();
        let sender = &senders[worker_id];
        let shred_bytes_meta = ShredBytesMeta {
            shred_bytes: Arc::new(buffer.to_vec()),
            received_at_micros: Some(*received_at_micros),
        };
        match sender.try_send(shred_bytes_meta) {
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return Err(anyhow::anyhow!("Channel full, backpressure detected"));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(anyhow::anyhow!("Channel disconnected"));
            }
        };

        Ok(())
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
            // Get buffer size
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &mut recv_buf_size as *mut _ as *mut libc::c_void,
                &mut len,
            );

            // Get queued bytes
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
