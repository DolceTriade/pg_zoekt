use anyhow::{Context, Result};
use pgrx::prelude::*;
/// Storing stuff
use std::collections::BTreeMap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

pub const TARGET_SEGMENTS: usize = 10;

pub mod decode;
pub mod encode;
pub mod pgbuffer;
pub mod wal;

pub const VERSION: u16 = 1;
pub const ROOT_MAGIC: u32 = u32::from_ne_bytes(*b"pZKT");
pub const BLOCK_MAGIC: u32 = u32::from_ne_bytes(*b"sZKT");
pub const WAL_MAGIC: u32 = u32::from_ne_bytes(*b"wZKT");
pub const WAL_BUCKET_MAGIC: u16 = u16::from_ne_bytes(*b"WL");
pub const POSTING_PAGE_MAGIC: u32 = u32::from_ne_bytes(*b"oZKT");

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
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
        let blk =
            unsafe { ((*value).ip_blkid.bi_hi as u32) << 16 | (*value).ip_blkid.bi_lo as u32 };
        let off = unsafe { (*value).ip_posid };
        Ok(Self {
            block_number: blk,
            offset: off,
        })
    }
}

#[derive(Debug, FromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct Segment {
    pub block: u32,
    pub size: u64,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct Segments {
    pub entries: [Segment],
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct BlockPointer {
    pub min_trigram: u32,
    pub block: u32,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct BlockHeader {
    pub magic: u32,
    pub level: u8,
    pub num_entries: u32,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct IndexEntry {
    pub trigram: u32,

    pub block: u32,  // The physical block where data starts
    pub offset: u16, // Where inside that block (0..8192)

    pub data_length: u32,

    pub frequency: u32,
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable)]
#[repr(C, packed)]
pub struct IndexList {
    pub entries: [IndexEntry],
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

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct PostingPageHeader {
    pub magic: u32,
    pub next_block: u32,
    pub next_offset: u16,
    pub free: u16,
}

#[derive(Debug, TryFromBytes, IntoBytes, KnownLayout, Unaligned, Immutable, Clone, Copy)]
#[repr(C, packed)]
pub struct CompressedBlockHeader {
    // Max of 128 docs per batch
    pub num_docs: u8,
    // hopefully good enough, we'll see.
    pub docs_blk_len: u16,
    pub docs_off_len: u16,

    pub counts_len: u16,

    pub pos_len: u16,

    pub flags_len: u16,
}

struct SegmentCursor {
    entries: Vec<IndexEntry>,
    idx: usize,
}

impl SegmentCursor {
    fn current_entry(&self) -> Option<&IndexEntry> {
        self.entries.get(self.idx)
    }

    fn advance(&mut self) {
        self.idx += 1;
    }
}

fn read_segment_entries(rel: pg_sys::Relation, segment: &Segment) -> Result<Vec<IndexEntry>> {
    let mut buf = pgbuffer::BlockBuffer::acquire(rel, segment.block);
    let header = buf.as_struct::<BlockHeader>(0).context("block header")?;
    if header.magic != BLOCK_MAGIC {
        anyhow::bail!("invalid block magic while merging");
    }
    let entries = buf
        .as_struct_with_elems::<IndexList>(
            std::mem::size_of::<BlockHeader>(),
            header.num_entries as usize,
        )
        .context("index entries")?;
    Ok(entries.entries[..header.num_entries as usize].to_vec())
}

fn peek_next_trigram(cursors: &[SegmentCursor]) -> Option<u32> {
    cursors
        .iter()
        .filter_map(|c| c.current_entry().map(|entry| entry.trigram))
        .min()
}

fn merge_entry_postings(
    rel: pg_sys::Relation,
    entries: &[IndexEntry],
) -> Result<BTreeMap<ItemPointer, Vec<crate::trgm::Occurance>>> {
    let mut docs: BTreeMap<ItemPointer, Vec<crate::trgm::Occurance>> = BTreeMap::new();
    for entry in entries {
        for posting in unsafe { crate::storage::decode::decode_postings(rel, entry)? } {
            let list = docs.entry(posting.tid).or_insert_with(Vec::new);
            for (position, flags) in posting.positions {
                let mut occ = crate::trgm::Occurance(position);
                occ.set_flags(flags);
                list.push(occ);
            }
        }
    }
    Ok(docs)
}

fn flush_collector(
    rel: pg_sys::Relation,
    collector: &mut crate::trgm::Collector,
    target: &mut Vec<Segment>,
) -> Result<()> {
    let trgms = collector.take_trgms();
    if trgms.is_empty() {
        return Ok(());
    }
    let mut segments = encode::Encoder::encode_trgms(rel, &trgms)?;
    target.append(&mut segments);
    Ok(())
}

pub fn merge(
    rel: pg_sys::Relation,
    segments: &[Segment],
    target_segments: usize,
    flush_threshold: usize,
) -> Result<Vec<Segment>> {
    let target_segments = target_segments.max(1);
    if segments.len() <= target_segments {
        return Ok(segments.to_vec());
    }

    let total_bytes = segments
        .iter()
        .map(|segment| segment.size)
        .sum::<u64>()
        .min(usize::MAX as u64) as usize;
    let per_segment_target = std::cmp::max(1usize, total_bytes / target_segments);

    let mut cursors = Vec::new();
    for segment in segments {
        let entries = read_segment_entries(rel, segment)?;
        if !entries.is_empty() {
            cursors.push(SegmentCursor { entries, idx: 0 });
        }
    }

    if cursors.is_empty() {
        return Ok(Vec::new());
    }

    let mut collector = crate::trgm::Collector::new();
    let mut bytes_since_flush = 0usize;
    let mut result = Vec::new();

    while let Some(trigram) = peek_next_trigram(&cursors) {
        let mut group_entries = Vec::new();
        for cursor in cursors.iter_mut() {
            while let Some(entry) = cursor.current_entry() {
                if entry.trigram != trigram {
                    break;
                }
                group_entries.push(*entry);
                cursor.advance();
            }
        }
        let mut postings = merge_entry_postings(rel, &group_entries)?;
        for (doc, occs) in postings.iter_mut() {
            occs.sort_unstable_by_key(|occ| occ.position());
            collector.add_occurrences(trigram, *doc, occs);
        }
        bytes_since_flush += group_entries
            .iter()
            .map(|entry| entry.data_length as usize)
            .sum::<usize>();
        if collector.memory_usage() >= flush_threshold || bytes_since_flush >= per_segment_target {
            flush_collector(rel, &mut collector, &mut result)?;
            bytes_since_flush = 0;
        }
    }

    if collector.memory_usage() > 0 {
        flush_collector(rel, &mut collector, &mut result)?;
    }

    Ok(result)
}
