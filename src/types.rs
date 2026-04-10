use solana_ledger::shred::{Shred, ShredType};

/// A received shred kept as raw bytes + pre-parsed header metadata.
///
/// Used only for the receiver → fec-worker hop. The fec worker parses it
/// into a full `Shred` immediately on arrival so recovery-time doesn't pay
/// the parse cost in a burst.
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

/// A fully-parsed shred with its arrival timestamp. Lives in per-FEC-set and
/// per-slot accumulators and is what downstream workers read.
#[derive(Debug, Clone)]
pub struct ShredMeta {
    pub shred: Shred,
    pub received_at_micros: Option<u64>,
}
