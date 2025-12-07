/// Storing stuff
pub mod encode;
pub mod pgbuffer;
pub mod wal;

pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");
pub const WAL_MAGIC: u32 = u32::from_ne_bytes(*b"wZKT");
pub const WAL_BUCKET_MAGIC: u32 = u32::from_ne_bytes(*b"bZKT");

#[repr(C)]
pub struct RootBlockList {
    pub magic: u32,
    pub version: u16,
    pub wal_block: u32,
    pub num_segments: u32,
    // Blocks...
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[repr(C)]
pub struct ItemPointer {
    pub block_number: pgrx::pg_sys::BlockNumber,
    pub offset: pgrx::pg_sys::OffsetNumber,
}

#[repr(C)]
pub struct Block {
    pub block: u32,
    pub size: u64,
}

#[repr(C)]
pub struct BlockPointer {
    pub min_trigram: u32,
    pub block: u32,
}

#[repr(C)]
pub struct Location {
    block: u32,
    offset: u16,
}

#[repr(C)]
pub struct BlockHeader {
    pub magic: u32,
    pub level: u8,
    pub num_entries: u32,
}

#[repr(C)]
pub struct IndexEntry {
    pub trigram: u32,

    pub block: u32,  // The physical block where data starts
    pub offset: u16, // Where inside that block (0..8192)

    pub data_length: u32,

    pub frequency: u32,
}

#[repr(C)]
pub struct WALBuckets {
    pub magic: u32,
    pub buckets: [u32; 256],
}

#[repr(C)]
pub struct WALBucket {
    pub magic: u32,
    pub next_block: u32,
}

#[repr(C)]
pub struct WALTrigram {
    pub trigram: u32,
    pub num_entries: u32,
}

#[repr(C)]
pub struct WALEntry {
    pub ctid: ItemPointer,
    pub num_positions: u32,
}
