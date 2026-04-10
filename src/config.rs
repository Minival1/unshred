use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnshredConfig {
    pub bind_address: String,
    pub num_fec_workers: Option<u8>,
    pub num_batch_workers: Option<u8>,
}

impl Default for UnshredConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8001".to_string(),
            num_fec_workers: None,
            num_batch_workers: None,
        }
    }
}
