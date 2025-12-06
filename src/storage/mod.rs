/// Storing stuff

pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");

#[repr(C, packed)]
pub struct RootBlockList {
    pub magic: u32,
    pub version: u16,
    pub num_segments: u32,
    // Blocks...
}

pub struct Block {
    pub block: u32,
    pub size: u64,
}

#[repr(C, packed)]
pub struct BlockPointer {
    pub min_trigram: [u8; 12],
    pub block: u32,
}

#[repr(C, packed)]
pub struct Location {
    block: u32,
    offset: u16,
}

#[repr(C, packed)]
pub struct BlockHeader {
    pub magic: u32,
    pub level: u8,
    pub num_entries: u32,
}

#[repr(C)]
pub struct IndexEntry {
    pub key_bytes: [u8; 12],

    pub start_blkno: u32,   // The physical block where data starts
    pub offset_in_blk: u16, // Where inside that block (0..8192)

    pub data_length: u32,

    pub frequency: u32,
}
