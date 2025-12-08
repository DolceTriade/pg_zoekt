/// Storing stuff
pub mod encode;
pub mod pgbuffer;
pub mod wal;

pub const VERSION: u16 = 1;
pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");
pub const WAL_MAGIC: u32 = u32::from_ne_bytes(*b"wZKT");
pub const WAL_BUCKET_MAGIC: u16 = u16::from_ne_bytes(*b"WL");

#[derive(Debug)]
#[repr(C)]
pub struct RootBlockList {
    pub magic: u32,
    pub version: u16,
    pub wal_block: u32,
    pub num_segments: u32,
    // Segments...
    pub segments: [Segment; 0],
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
#[repr(C)]
pub struct ItemPointer {
    pub block_number: pgrx::pg_sys::BlockNumber,
    pub offset: pgrx::pg_sys::OffsetNumber,
}

#[derive(Debug)]
#[repr(C)]
pub struct Segment {
    pub block: u32,
    pub size: u64,
}

#[derive(Debug)]
#[repr(C)]
pub struct BlockPointer {
    pub min_trigram: u32,
    pub block: u32,
}

#[derive(Debug)]
#[repr(C)]
pub struct BlockHeader {
    pub magic: u32,
    pub level: u8,
    pub num_entries: u32,
}

#[derive(Debug)]
#[repr(C)]
pub struct IndexEntry {
    pub trigram: u32,

    pub block: u32,  // The physical block where data starts
    pub offset: u16, // Where inside that block (0..8192)

    pub data_length: u32,

    pub frequency: u32,
}

#[derive(Debug)]
#[repr(C)]
pub struct WALBuckets {
    pub magic: u32,
    pub buckets: [u32; 256],
}

#[derive(Debug)]
#[repr(C)]
pub struct WALBucket {
    pub magic: u16,
    pub free: u16,
    pub next_block: u32,

}

#[derive(Debug)]
#[repr(C)]
pub struct WALTrigram {
    pub trigram: u32,
    pub num_entries: u32,
}

#[derive(Debug)]
#[repr(C)]
pub struct WALEntry {
    pub ctid: ItemPointer,
    pub num_positions: u32,
}
