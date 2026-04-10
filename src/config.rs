use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnshredConfig {
    pub bind_address: String,
    pub num_fec_workers: Option<u8>,
    pub num_batch_workers: Option<u8>,
    /// Number of UDP receiver threads (each with its own SO_REUSEPORT socket).
    /// Defaults to 2. Raise to 4 on very high pps (>500k).
    pub num_receivers: Option<u8>,
    /// Number of dispatcher tasks that accumulate completed FEC sets into
    /// per-slot state and emit batches. Defaults to 2. Shards by `slot % N`
    /// so every FEC set of the same slot lands on the same dispatcher.
    pub num_dispatchers: Option<u8>,
}

impl Default for UnshredConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8001".to_string(),
            num_fec_workers: None,
            num_batch_workers: None,
            num_receivers: None,
            num_dispatchers: None,
        }
    }
}
