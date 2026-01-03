use std::io::Cursor;

use anyhow::{Context, Result};
use roaring::RoaringTreemap;

use crate::storage::pgbuffer::BlockBuffer;
use pgrx::pg_sys;

use super::{ItemPointer, RootBlockList, TOMBSTONE_PAGE_MAGIC};

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    bitmap: RoaringTreemap,
}

impl Snapshot {
    pub fn contains(&self, tid: ItemPointer) -> bool {
        self.bitmap.contains(encode_tid(tid))
    }

    pub fn insert(&mut self, tid: ItemPointer) -> bool {
        self.bitmap.insert(encode_tid(tid))
    }

    pub fn extend<I>(&mut self, tids: I) -> u64
    where
        I: IntoIterator<Item = ItemPointer>,
    {
        let mut added = 0;
        for tid in tids {
            if self.insert(tid) {
                added += 1;
            }
        }
        added
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

#[derive(
    Debug, zerocopy::TryFromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable,
)]
#[repr(C, packed)]
struct TombstonePageHeader {
    magic: u32,
    used: u16,
    next_block: u32,
}

const fn payload_capacity() -> usize {
    crate::storage::pgbuffer::SPECIAL_SIZE - std::mem::size_of::<TombstonePageHeader>()
}

fn encode_tid(tid: ItemPointer) -> u64 {
    ((tid.block_number as u64) << 16) | tid.offset as u64
}

pub fn load_snapshot(rel: pg_sys::Relation) -> Result<Snapshot> {
    let root = BlockBuffer::acquire(rel, 0)?;
    let rbl = root.as_struct::<RootBlockList>(0).context("root header")?;
    load_internal(rel, rbl)
}

pub fn load_snapshot_for_root(rel: pg_sys::Relation, root: &RootBlockList) -> Result<Snapshot> {
    load_internal(rel, root)
}

fn load_internal(rel: pg_sys::Relation, root: &RootBlockList) -> Result<Snapshot> {
    if root.tombstone_block == pg_sys::InvalidBlockNumber || root.tombstone_bytes == 0 {
        return Ok(Snapshot::default());
    }

    let mut remaining = root.tombstone_bytes as usize;
    let mut block = root.tombstone_block;
    let mut bytes = Vec::with_capacity(remaining);

    while block != pg_sys::InvalidBlockNumber && remaining > 0 {
        let page = BlockBuffer::acquire(rel, block)?;
        let header = page
            .as_struct::<TombstonePageHeader>(0)
            .context("tombstone header")?;
        if header.magic != TOMBSTONE_PAGE_MAGIC {
            anyhow::bail!("invalid tombstone page magic");
        }
        let used = header.used as usize;
        if used > payload_capacity() {
            anyhow::bail!("tombstone page payload overflow");
        }
        let buf = page.as_ref();
        let start = std::mem::size_of::<TombstonePageHeader>();
        let end = start + used;
        bytes.extend_from_slice(&buf[start..end]);
        block = header.next_block;
        remaining = remaining.saturating_sub(used);
    }

    bytes.truncate(root.tombstone_bytes as usize);
    if bytes.is_empty() {
        return Ok(Snapshot::default());
    }

    let mut cursor = Cursor::new(bytes);
    let bitmap =
        RoaringTreemap::deserialize_from(&mut cursor).context("deserialize tombstone bitmap")?;
    Ok(Snapshot { bitmap })
}

pub fn apply_deletions<I>(rel: pg_sys::Relation, tids: I) -> Result<u64>
where
    I: IntoIterator<Item = ItemPointer>,
{
    let mut root = BlockBuffer::aquire_mut(rel, 0)?;
    let rbl = root
        .as_struct_mut::<RootBlockList>(0)
        .context("root header")?;
    let mut snapshot = load_internal(rel, rbl)?;
    let added = snapshot.extend(tids);
    if added > 0 {
        persist(rel, rbl, &snapshot)?;
    }
    Ok(added)
}

fn persist(rel: pg_sys::Relation, root: &mut RootBlockList, snapshot: &Snapshot) -> Result<()> {
    if snapshot.is_empty() {
        root.tombstone_block = pg_sys::InvalidBlockNumber;
        root.tombstone_bytes = 0;
        return Ok(());
    }

    let mut data = Vec::new();
    snapshot
        .bitmap
        .serialize_into(&mut data)
        .context("serialize tombstones")?;

    let mut existing = collect_chain(rel, root.tombstone_block)?;
    let mut used_blocks = Vec::new();
    let mut cursor = data.as_slice();
    let chunk_cap = payload_capacity();

    while !cursor.is_empty() {
        let block = if let Some(blk) = existing.pop() {
            blk
        } else {
            super::allocate_block(rel).block_number()
        };
        let chunk_len = cursor.len().min(chunk_cap);
        {
            let mut page = BlockBuffer::aquire_mut(rel, block)?;
            let header = page
                .as_struct_mut::<TombstonePageHeader>(0)
                .context("tombstone header")?;
            header.magic = TOMBSTONE_PAGE_MAGIC;
            header.used = chunk_len as u16;
            header.next_block = pg_sys::InvalidBlockNumber;
            let buf = page.as_mut();
            let start = std::mem::size_of::<TombstonePageHeader>();
            let end = start + chunk_len;
            buf[start..end].copy_from_slice(&cursor[..chunk_len]);
            if end < buf.len() {
                buf[end..].fill(0);
            }
        }
        used_blocks.push(block);
        cursor = &cursor[chunk_len..];
    }

    for window in used_blocks.windows(2) {
        let mut page = BlockBuffer::aquire_mut(rel, window[0])?;
        let header = page
            .as_struct_mut::<TombstonePageHeader>(0)
            .context("tombstone header")?;
        header.next_block = window[1];
    }

    root.tombstone_block = *used_blocks.first().unwrap();
    root.tombstone_bytes = data.len() as u32;
    Ok(())
}

fn collect_chain(rel: pg_sys::Relation, start: u32) -> Result<Vec<u32>> {
    if start == pg_sys::InvalidBlockNumber {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut block = start;
    while block != pg_sys::InvalidBlockNumber {
        let page = BlockBuffer::acquire(rel, block)?;
        let header = page
            .as_struct::<TombstonePageHeader>(0)
            .context("tombstone header")?;
        if header.magic != TOMBSTONE_PAGE_MAGIC {
            anyhow::bail!("invalid tombstone page magic");
        }
        out.push(block);
        block = header.next_block;
    }
    out.reverse();
    Ok(out)
}
