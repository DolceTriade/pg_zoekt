use anyhow::Context;
use delta_encoding::DeltaDecoderExt;
use pgrx::prelude::*;
use zerocopy::TryFromBytes;

use super::{CompressedBlockHeader, IndexEntry, ItemPointer};
use crate::storage::pgbuffer::BlockBuffer;

#[derive(Debug, Clone)]
pub struct DocPosting {
    pub tid: ItemPointer,
    pub positions: Vec<(u32, u8)>, // (position, flags)
}

#[derive(Debug)]
struct FlagsCursor<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: usize,
    mode: FlagsMode<'a>,
}

#[derive(Debug)]
enum FlagsMode<'a> {
    Repeat(u8),
    Literal { slice: &'a [u8], idx: usize },
}

#[derive(Debug)]
pub struct PostingCursor {
    _buf: BlockBuffer,
    tids: Vec<ItemPointer>,
    counts: Vec<u32>,

    pos_cursor: stream_vbyte::decode::cursor::DecodeCursor<'static>,
    pos_buf: Vec<u32>,
    pos_buf_idx: usize,
    pos_buf_len: usize,
    pos_acc: u32,
    flags: FlagsCursor<'static>,
    doc_idx: usize,

    current: Option<DocPosting>,
}

fn take_slice<'a>(page: &'a [u8], cursor: &mut usize, len: usize) -> anyhow::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .context("overflow while slicing postings")?;
    if end > page.len() {
        anyhow::bail!("postings overrun");
    }
    let slice = &page[*cursor..end];
    *cursor = end;
    Ok(slice)
}

pub unsafe fn decode_postings(
    rel: pg_sys::Relation,
    entry: &IndexEntry,
) -> anyhow::Result<Vec<DocPosting>> {
    let mut buf = BlockBuffer::acquire(rel, entry.block);
    let page = buf.as_ref();
    let start = entry.offset as usize;
    if start >= page.len() {
        anyhow::bail!("offset out of bounds");
    }
    let header_bytes = page
        .get(start..start + std::mem::size_of::<CompressedBlockHeader>())
        .context("header slice")?;
    let hdr = *CompressedBlockHeader::try_ref_from_bytes(header_bytes)
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let mut cursor = start + std::mem::size_of::<CompressedBlockHeader>();

    let num_docs = hdr.num_docs as usize;
    let blk_bytes = take_slice(page, &mut cursor, hdr.docs_blk_len as usize)?;
    let mut blk_nums = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(blk_bytes, num_docs, &mut blk_nums);
    let blk_nums = blk_nums.into_iter().original().collect::<Vec<u32>>();

    let off_bytes = take_slice(page, &mut cursor, hdr.docs_off_len as usize)?;
    let mut offs = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(off_bytes, num_docs, &mut offs);

    let count_bytes = take_slice(page, &mut cursor, hdr.counts_len as usize)?;
    let mut counts = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(count_bytes, num_docs, &mut counts);

    let total_positions: usize = counts.iter().copied().map(|c| c as usize).sum();
    let pos_bytes = take_slice(page, &mut cursor, hdr.pos_len as usize)?;
    let mut positions = vec![0u32; total_positions];
    if total_positions > 0 {
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            pos_bytes,
            total_positions,
            &mut positions,
        );
    }
    let positions = positions.into_iter().original().collect::<Vec<u32>>();

    let flag_bytes = take_slice(page, &mut cursor, hdr.flags_len as usize)?;
    let flags = bitfield_rle::decode(flag_bytes.to_vec()).unwrap_or_default();

    let mut pos_cursor = 0usize;
    let mut postings = Vec::with_capacity(num_docs);
    for i in 0..num_docs {
        let num_pos = counts[i] as usize;
        let doc_positions = positions
            .get(pos_cursor..pos_cursor + num_pos)
            .unwrap_or(&[]);
        let doc_flags = flags.get(pos_cursor..pos_cursor + num_pos).unwrap_or(&[]);
        pos_cursor += num_pos;

        let mut pairs = Vec::with_capacity(doc_positions.len());
        for (p, f) in doc_positions
            .iter()
            .zip(doc_flags.iter().copied().chain(std::iter::repeat(0)))
        {
            pairs.push((*p, f));
        }

        postings.push(DocPosting {
            tid: ItemPointer {
                block_number: blk_nums[i],
                offset: offs[i] as u16,
            },
            positions: pairs,
        });
    }
    Ok(postings)
}

/// Stream only TIDs from a posting list into `out`, skipping positional materialization.
/// This is a fast path for simple patterns where we don't need positions/flags.
pub unsafe fn stream_tids(
    rel: pg_sys::Relation,
    entry: &IndexEntry,
    out: &mut Vec<ItemPointer>,
) -> anyhow::Result<()> {
    let buf = BlockBuffer::acquire(rel, entry.block);
    let page = buf.as_ref();
    let start = entry.offset as usize;
    if start >= page.len() {
        anyhow::bail!("offset out of bounds");
    }
    let header_bytes = page
        .get(start..start + std::mem::size_of::<CompressedBlockHeader>())
        .context("header slice")?;
    let hdr = *CompressedBlockHeader::try_ref_from_bytes(header_bytes)
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let mut cursor = start + std::mem::size_of::<CompressedBlockHeader>();

    let num_docs = hdr.num_docs as usize;

    let blk_bytes = take_slice(page, &mut cursor, hdr.docs_blk_len as usize)?;
    let mut blk_nums = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(blk_bytes, num_docs, &mut blk_nums);
    let blk_nums = blk_nums.into_iter().original().collect::<Vec<u32>>();

    let off_bytes = take_slice(page, &mut cursor, hdr.docs_off_len as usize)?;
    let mut offs = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(off_bytes, num_docs, &mut offs);

    // Decode counts to advance positions cursor, but don't retain positions.
    let count_bytes = take_slice(page, &mut cursor, hdr.counts_len as usize)?;
    let mut counts = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(count_bytes, num_docs, &mut counts);

    let pos_bytes = take_slice(page, &mut cursor, hdr.pos_len as usize)?;
    let total_positions: usize = counts.iter().copied().map(|c| c as usize).sum();
    let mut pos_cursor =
        stream_vbyte::decode::cursor::DecodeCursor::new(pos_bytes, total_positions);
    let mut delta = 0u32;
    let mut buf = vec![0u32; 128];
    let mut buf_len = 0usize;
    let mut buf_idx = 0usize;
    let mut next_delta = || -> anyhow::Result<u32> {
        if buf_idx >= buf_len {
            let decoded = pos_cursor.decode_slice::<stream_vbyte::x86::Ssse3>(&mut buf[..]);
            if decoded == 0 {
                anyhow::bail!("failed to decode positions");
            }
            buf_len = decoded;
            buf_idx = 0;
        }
        let v = buf[buf_idx];
        buf_idx += 1;
        Ok(v)
    };

    for i in 0..num_docs {
        let num_pos = counts[i] as usize;
        for _ in 0..num_pos {
            delta = delta.wrapping_add(next_delta()?);
        }
        out.push(ItemPointer {
            block_number: blk_nums[i],
            offset: offs[i] as u16,
        });
    }

    // skip flags payload
    let _ = take_slice(page, &mut cursor, hdr.flags_len as usize)?;
    Ok(())
}

impl PostingCursor {
    #[inline]
    fn decode_var_u64(buf: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
        let mut val = 0u64;
        let mut fac = 1u64;
        loop {
            let byte = *buf.get(*offset).context("flags varint overrun")?;
            *offset += 1;
            val = val.wrapping_add(fac.wrapping_mul((byte & 127) as u64));
            fac <<= 7;
            if byte & 128 == 0 {
                break;
            }
        }
        Ok(val)
    }

    fn next_flag(flags: &mut FlagsCursor<'static>) -> anyhow::Result<u8> {
        if flags.remaining == 0 {
            if flags.offset >= flags.buf.len() {
                anyhow::bail!("flags underrun");
            }
            let next = Self::decode_var_u64(flags.buf, &mut flags.offset)?;
            let repeat = (next & 1) != 0;
            let len = if repeat {
                (next >> 2) as usize
            } else {
                (next >> 1) as usize
            };
            if repeat {
                let v = if (next & 2) != 0 { 255u8 } else { 0u8 };
                flags.mode = FlagsMode::Repeat(v);
                flags.remaining = len;
            } else {
                let start = flags.offset;
                let end = start.checked_add(len).context("flags overflow")?;
                let slice = flags.buf.get(start..end).context("flags overrun")?;
                flags.offset = end;
                flags.mode = FlagsMode::Literal { slice, idx: 0 };
                flags.remaining = len;
            }
        }

        flags.remaining -= 1;
        match &mut flags.mode {
            FlagsMode::Repeat(v) => Ok(*v),
            FlagsMode::Literal { slice, idx } => {
                let b = *slice.get(*idx).context("flags literal overrun")?;
                *idx += 1;
                Ok(b)
            }
        }
    }

    /// Build a streaming cursor for a single index entry. The cursor keeps only the
    /// doc headers in memory and streams positions as it advances.
    pub unsafe fn new(rel: pg_sys::Relation, entry: &IndexEntry) -> anyhow::Result<Self> {
        let mut buf = BlockBuffer::acquire(rel, entry.block);
        let page = buf.as_ref();
        let start = entry.offset as usize;
        if start >= page.len() {
            anyhow::bail!("offset out of bounds");
        }
        let header_bytes = page
            .get(start..start + std::mem::size_of::<CompressedBlockHeader>())
            .context("header slice")?;
        let hdr = *CompressedBlockHeader::try_ref_from_bytes(header_bytes)
            .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
        let mut cursor = start + std::mem::size_of::<CompressedBlockHeader>();

        let num_docs = hdr.num_docs as usize;

        let blk_bytes = take_slice(page, &mut cursor, hdr.docs_blk_len as usize)?;
        let mut blk_nums = vec![0u32; num_docs];
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            blk_bytes,
            num_docs,
            &mut blk_nums,
        );
        let blk_nums = blk_nums.into_iter().original().collect::<Vec<u32>>();

        let off_bytes = take_slice(page, &mut cursor, hdr.docs_off_len as usize)?;
        let mut offs = vec![0u32; num_docs];
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(off_bytes, num_docs, &mut offs);

        let count_bytes = take_slice(page, &mut cursor, hdr.counts_len as usize)?;
        let mut counts = vec![0u32; num_docs];
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            count_bytes,
            num_docs,
            &mut counts,
        );

        let total_positions: usize = counts.iter().copied().map(|c| c as usize).sum();
        let pos_bytes = take_slice(page, &mut cursor, hdr.pos_len as usize)?;
        // Safe because `buf` is owned by the cursor and the page bytes outlive `self`.
        let pos_bytes_static: &'static [u8] = std::mem::transmute(pos_bytes);
        let pos_cursor =
            stream_vbyte::decode::cursor::DecodeCursor::new(pos_bytes_static, total_positions);

        let flag_bytes = take_slice(page, &mut cursor, hdr.flags_len as usize)?;
        let flag_bytes_static: &'static [u8] = std::mem::transmute(flag_bytes);
        let flags = FlagsCursor {
            buf: flag_bytes_static,
            offset: 0,
            remaining: 0,
            mode: FlagsMode::Repeat(0),
        };

        Ok(Self {
            _buf: buf,
            tids: blk_nums
                .into_iter()
                .zip(offs.into_iter())
                .map(|(blk, off)| ItemPointer {
                    block_number: blk,
                    offset: off as u16,
                })
                .collect(),
            counts,
            pos_cursor,
            pos_buf: vec![0; 128],
            pos_buf_idx: 0,
            pos_buf_len: 0,
            pos_acc: 0,
            flags,
            doc_idx: 0,
            current: None,
        })
    }

    fn next_pos_delta(&mut self) -> anyhow::Result<u32> {
        if self.pos_buf_idx >= self.pos_buf_len {
            let decoded = self
                .pos_cursor
                .decode_slice::<stream_vbyte::x86::Ssse3>(&mut self.pos_buf[..]);
            if decoded == 0 {
                anyhow::bail!("unexpected decoded len");
            }
            self.pos_buf_len = decoded;
            self.pos_buf_idx = 0;
        }
        let v = self.pos_buf[self.pos_buf_idx];
        self.pos_buf_idx += 1;
        Ok(v)
    }

    pub fn current(&self) -> Option<&DocPosting> {
        self.current.as_ref()
    }

    pub fn current_tid(&self) -> Option<ItemPointer> {
        self.current.as_ref().map(|d| d.tid)
    }

    pub fn advance(&mut self) -> anyhow::Result<bool> {
        if self.doc_idx >= self.tids.len() {
            self.current = None;
            return Ok(false);
        }

        let tid = self.tids[self.doc_idx];
        let num_pos = self.counts[self.doc_idx] as usize;

        let mut positions = if let Some(doc) = self.current.take() {
            let mut p = doc.positions;
            p.clear();
            p
        } else {
            Vec::with_capacity(num_pos.max(4))
        };
        positions.clear();

        if num_pos > 0 {
            for _ in 0..num_pos {
                let delta = self.next_pos_delta()?;
                self.pos_acc = self.pos_acc.wrapping_add(delta);
                let flag = Self::next_flag(&mut self.flags)?;
                positions.push((self.pos_acc, flag));
            }
        }

        self.doc_idx += 1;
        self.current = Some(DocPosting { tid, positions });
        Ok(true)
    }

    /// Advance one document and return whether it contains `target_pos` with matching flags.
    /// This avoids materializing the per-document positions vector.
    pub fn advance_check_position(
        &mut self,
        target_pos: u32,
        pattern_flag: u8,
        case_sensitive: bool,
    ) -> anyhow::Result<Option<(ItemPointer, bool)>> {
        if self.doc_idx >= self.tids.len() {
            self.current = None;
            return Ok(None);
        }

        let tid = self.tids[self.doc_idx];
        let num_pos = self.counts[self.doc_idx] as usize;
        let mut found = false;

        for _ in 0..num_pos {
            let delta = self.next_pos_delta()?;
            self.pos_acc = self.pos_acc.wrapping_add(delta);
            let flag = Self::next_flag(&mut self.flags)?;
            if self.pos_acc == target_pos
                && (!case_sensitive || (flag & 0b111) == (pattern_flag & 0b111))
            {
                found = true;
            }
        }
        self.doc_idx += 1;
        self.current = None;
        Ok(Some((tid, found)))
    }
}
