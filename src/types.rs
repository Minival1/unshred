use solana_ledger::shred::ShredType;

/// A received shred kept as raw bytes + pre-parsed header metadata.
///
/// Avoids calling `Shred::new_from_serialized_shred` until actually needed
/// (which is only during FEC recovery). For the natural-completion path
/// (all data shreds received) we never construct a `Shred` object at all.
#[derive(Debug)]
pub struct RawShred {
    pub bytes: Vec<u8>,
    pub slot: u64,
    pub index: u32,
    pub fec_set_index: u32,
    pub shred_type: ShredType,
    /// Data-shred flags byte (offset 85). Only meaningful for `ShredType::Data`.
    pub data_flags: u8,
    /// Number of data shreds in this FEC set. Only populated for code shreds
    /// (read from coding-header offset 83..85).
    pub num_data_shreds: Option<u16>,
    pub received_at_micros: Option<u64>,
}
