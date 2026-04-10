use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ShredBytesMeta {
    pub shred_bytes: Arc<Vec<u8>>,
    pub received_at_micros: Option<u64>,
}
