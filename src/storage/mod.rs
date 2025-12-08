/// Storing stuff
use zerocopy::{FromBytes, IntoBytes, KnownLayout, TryFromBytes, Unaligned};
use pgrx::prelude::*;

pub mod encode;
pub mod pgbuffer;
pub mod wal;

pub const VERSION: u16 = 1;
pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");
pub const WAL_MAGIC: u32 = u32::from_ne_bytes(*b"wZKT");
pub const WAL_BUCKET_MAGIC: u16 = u16::from_ne_bytes(*b"WL");

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct RootBlockList {
    pub magic: u32,
    pub version: u16,
    pub wal_block: u32,
    pub num_segments: u32,
    // Segments...
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    PartialOrd,
    Ord,
    TryFromBytes,
    IntoBytes,
    KnownLayout,
    Unaligned,
)]
#[repr(C, packed)]
pub struct ItemPointer {
    pub block_number: pgrx::pg_sys::BlockNumber,
    pub offset: pgrx::pg_sys::OffsetNumber,
}

impl TryFrom<pg_sys::ItemPointer> for ItemPointer {
    type Error = anyhow::Error;

    fn try_from(value: pg_sys::ItemPointer) -> anyhow::Result<Self> {
        if value.is_null() {
            anyhow::bail!("ItemPointer is null!");
        }
        let blk = unsafe { 
            ((*value).ip_blkid.bi_hi as u32) << 16 | (*value).ip_blkid.bi_lo as u32
        };
        let off = unsafe {
            (*value).ip_posid
        };
        Ok(Self {
            block_number: blk,
            offset: off,
        })
    }
}

#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct Segment {
    pub block: u32,
    pub size: u64,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout)]
#[repr(C, packed)]
pub struct BlockPointer {
    pub min_trigram: u32,
    pub block: u32,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct BlockHeader {
    pub magic: u32,
    pub level: u8,
    pub num_entries: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct IndexEntry {
    pub trigram: u32,

    pub block: u32,  // The physical block where data starts
    pub offset: u16, // Where inside that block (0..8192)

    pub data_length: u32,

    pub frequency: u32,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct IndexList {
    entries: [IndexEntry],
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct WALBuckets {
    pub magic: u32,
    pub buckets: [u32; 256],
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout)]
#[repr(C, packed)]
pub struct WALBucket {
    pub magic: u16,
    pub free: u16,
    pub next_block: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout)]
#[repr(C, packed)]
pub struct WALTrigram {
    pub trigram: u32,
    pub num_entries: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct WALEntry {
    pub ctid: ItemPointer,
    pub num_positions: u32,
}
