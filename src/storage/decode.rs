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
    reader: PostingReader,

    chunk_tids: Vec<ItemPointer>,
    chunk_counts: Vec<u32>,
    chunk_doc_idx: usize,

    pos_storage: Vec<u8>,
    flag_storage: Vec<u8>,
    pos_cursor: stream_vbyte::decode::cursor::DecodeCursor<'static>,
    pos_buf: Vec<u32>,
    pos_buf_idx: usize,
    pos_buf_len: usize,
    pos_acc: u32,
    flags: FlagsCursor<'static>,

    current: Option<DocPosting>,
}

#[derive(Debug)]
struct PostingReader {
    rel: pg_sys::Relation,
    buf: BlockBuffer,
    header: super::PostingPageHeader,
    cursor: usize,
    page_free: usize,
    remaining: usize,
}

impl PostingReader {
    unsafe fn new(rel: pg_sys::Relation, entry: &IndexEntry) -> anyhow::Result<Self> {
        let buf = BlockBuffer::acquire(rel, entry.block);
        let header_copy = {
            let header = buf
                .as_struct::<super::PostingPageHeader>(0)
                .context("posting page header")?;
            *header
        };
        if header_copy.magic != super::POSTING_PAGE_MAGIC {
            anyhow::bail!("invalid posting page magic");
        }
        let start = entry.offset as usize;
        if start < header_copy.next_offset as usize || start > header_copy.free as usize {
            anyhow::bail!("offset out of bounds");
        }
        Ok(Self {
            rel,
            buf,
            header: header_copy,
            cursor: start,
            page_free: header_copy.free as usize,
            remaining: entry.data_length as usize,
        })
    }

    fn has_remaining(&self) -> bool {
        self.remaining > 0
    }

    fn take_slice<'a>(&'a mut self, len: usize) -> anyhow::Result<&'a [u8]> {
        if len == 0 {
            return Ok(&[]);
        }
        if len > self.remaining {
            anyhow::bail!("posting slice exceeds declared length");
        }
        self.ensure_page_space(len)?;
        let page = self.buf.as_ref();
        let end = self.cursor + len;
        let slice = &page[self.cursor..end];
        self.cursor = end;
        self.remaining -= len;
        Ok(slice)
    }

    fn ensure_page_space(&mut self, len: usize) -> anyhow::Result<()> {
        loop {
            let available = self.page_free.saturating_sub(self.cursor);
            if len <= available {
                return Ok(());
            }
            if available != 0 {
                anyhow::bail!("posting data spans page boundary");
            }
            self.advance_page()?;
        }
    }

    fn advance_page(&mut self) -> anyhow::Result<()> {
        let next_block = self.header.next_block;
        if next_block == pg_sys::InvalidBlockNumber {
            anyhow::bail!("posting chain truncated");
        }
        let next_buf = BlockBuffer::acquire(self.rel, next_block);
        let header_copy = {
            let header = next_buf
                .as_struct::<super::PostingPageHeader>(0)
                .context("posting page header")?;
            *header
        };
        if header_copy.magic != super::POSTING_PAGE_MAGIC {
            anyhow::bail!("invalid posting page magic");
        }
        self.buf = next_buf;
        self.header = header_copy;
        self.cursor = header_copy.next_offset as usize;
        self.page_free = header_copy.free as usize;
        Ok(())
    }
}

pub unsafe fn decode_postings(
    rel: pg_sys::Relation,
    entry: &IndexEntry,
) -> anyhow::Result<Vec<DocPosting>> {
    if entry.data_length == 0 {
        return Ok(Vec::new());
    }
    let mut reader = unsafe { PostingReader::new(rel, entry)? };
    let mut postings = Vec::new();
    while reader.has_remaining() {
        postings.append(&mut decode_chunk(&mut reader)?);
    }
    Ok(postings)
}

fn decode_chunk(reader: &mut PostingReader) -> anyhow::Result<Vec<DocPosting>> {
    let header_bytes = reader.take_slice(std::mem::size_of::<CompressedBlockHeader>())?;
    let hdr = *CompressedBlockHeader::try_ref_from_bytes(header_bytes)
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let num_docs = hdr.num_docs as usize;

    let blk_bytes = reader.take_slice(hdr.docs_blk_len as usize)?;
    let mut blk_nums = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(blk_bytes, num_docs, &mut blk_nums);
    let blk_nums = blk_nums.into_iter().original().collect::<Vec<u32>>();

    let off_bytes = reader.take_slice(hdr.docs_off_len as usize)?;
    let mut offs = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(off_bytes, num_docs, &mut offs);

    let count_bytes = reader.take_slice(hdr.counts_len as usize)?;
    let mut counts = vec![0u32; num_docs];
    stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(count_bytes, num_docs, &mut counts);

    let total_positions: usize = counts.iter().copied().map(|c| c as usize).sum();
    let pos_bytes = reader.take_slice(hdr.pos_len as usize)?;
    let mut positions = vec![0u32; total_positions];
    if total_positions > 0 {
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            pos_bytes,
            total_positions,
            &mut positions,
        );
    }
    let positions = positions.into_iter().original().collect::<Vec<u32>>();

    let flag_bytes = reader.take_slice(hdr.flags_len as usize)?;
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
    /// doc headers for the current compressed chunk in memory and streams positions as it advances.
    pub unsafe fn new(rel: pg_sys::Relation, entry: &IndexEntry) -> anyhow::Result<Self> {
        let reader = unsafe { PostingReader::new(rel, entry)? };
        let pos_cursor = stream_vbyte::decode::cursor::DecodeCursor::new(&[], 0);
        let flags = FlagsCursor {
            buf: &[],
            offset: 0,
            remaining: 0,
            mode: FlagsMode::Repeat(0),
        };
        let mut cursor = Self {
            reader,
            chunk_tids: Vec::new(),
            chunk_counts: Vec::new(),
            chunk_doc_idx: 0,
            pos_storage: Vec::new(),
            flag_storage: Vec::new(),
            pos_cursor,
            pos_buf: vec![0; 128],
            pos_buf_idx: 0,
            pos_buf_len: 0,
            pos_acc: 0,
            flags,
            current: None,
        };
        cursor.load_next_chunk()?;
        Ok(cursor)
    }

    fn load_next_chunk(&mut self) -> anyhow::Result<bool> {
        self.chunk_tids.clear();
        self.chunk_counts.clear();
        self.chunk_doc_idx = 0;
        self.pos_storage.clear();
        self.flag_storage.clear();
        self.pos_buf_idx = 0;
        self.pos_buf_len = 0;
        self.pos_acc = 0;
        self.flags.remaining = 0;
        self.flags.offset = 0;

        if !self.reader.has_remaining() {
            return Ok(false);
        }

        let header_bytes = self
            .reader
            .take_slice(std::mem::size_of::<CompressedBlockHeader>())?;
        let hdr = *CompressedBlockHeader::try_ref_from_bytes(header_bytes)
            .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
        let num_docs = hdr.num_docs as usize;

        let blk_bytes = self.reader.take_slice(hdr.docs_blk_len as usize)?;
        let mut blk_nums = vec![0u32; num_docs];
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            blk_bytes,
            num_docs,
            &mut blk_nums,
        );
        let blk_nums = blk_nums.into_iter().original().collect::<Vec<u32>>();

        let off_bytes = self.reader.take_slice(hdr.docs_off_len as usize)?;
        let mut offs = vec![0u32; num_docs];
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(off_bytes, num_docs, &mut offs);

        let count_bytes = self.reader.take_slice(hdr.counts_len as usize)?;
        let mut counts = vec![0u32; num_docs];
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            count_bytes,
            num_docs,
            &mut counts,
        );
        let total_positions = counts.iter().copied().map(|c| c as usize).sum::<usize>();

        self.chunk_counts.extend(counts.iter().copied());
        for (blk, off) in blk_nums.into_iter().zip(offs.into_iter()) {
            self.chunk_tids.push(ItemPointer {
                block_number: blk,
                offset: off as u16,
            });
        }

        let pos_slice = self.reader.take_slice(hdr.pos_len as usize)?;
        self.pos_storage.extend_from_slice(pos_slice);
        let flag_slice = self.reader.take_slice(hdr.flags_len as usize)?;
        self.flag_storage.extend_from_slice(flag_slice);

        let pos_bytes_static: &'static [u8] = unsafe {
            let slice = self.pos_storage.as_slice();
            std::mem::transmute::<&[u8], &'static [u8]>(slice)
        };
        let flag_bytes_static: &'static [u8] = unsafe {
            let slice = self.flag_storage.as_slice();
            std::mem::transmute::<&[u8], &'static [u8]>(slice)
        };
        self.pos_cursor =
            stream_vbyte::decode::cursor::DecodeCursor::new(pos_bytes_static, total_positions);
        self.flags = FlagsCursor {
            buf: flag_bytes_static,
            offset: 0,
            remaining: 0,
            mode: FlagsMode::Repeat(0),
        };

        Ok(true)
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
        loop {
            if self.chunk_doc_idx >= self.chunk_tids.len() {
                if !self.load_next_chunk()? {
                    self.current = None;
                    return Ok(false);
                }
                continue;
            }

            let tid = self.chunk_tids[self.chunk_doc_idx];
            let mut total_needed_positions = self.chunk_counts[self.chunk_doc_idx] as usize;
            self.chunk_doc_idx += 1;

            while self.chunk_doc_idx < self.chunk_tids.len()
                && self.chunk_tids[self.chunk_doc_idx] == tid
            {
                total_needed_positions += self.chunk_counts[self.chunk_doc_idx] as usize;
                self.chunk_doc_idx += 1;
            }

            let mut positions = if let Some(doc) = self.current.take() {
                let mut p = doc.positions;
                p.clear();
                p
            } else {
                Vec::with_capacity(total_needed_positions.max(4))
            };

            for _ in 0..total_needed_positions {
                let delta = self.next_pos_delta()?;
                self.pos_acc = self.pos_acc.wrapping_add(delta);
                let flag = Self::next_flag(&mut self.flags)?;
                positions.push((self.pos_acc, flag));
            }

            self.current = Some(DocPosting { tid, positions });
            return Ok(true);
        }
    }

    /// Advance one document and return whether it contains `target_pos` with matching flags.
    /// This avoids materializing the per-document positions vector.
    pub fn advance_check_position(
        &mut self,
        target_pos: u32,
        pattern_flag: u8,
        case_sensitive: bool,
    ) -> anyhow::Result<Option<(ItemPointer, bool)>> {
        loop {
            if self.chunk_doc_idx >= self.chunk_tids.len() {
                if !self.load_next_chunk()? {
                    self.current = None;
                    return Ok(None);
                }
                continue;
            }

            let tid = self.chunk_tids[self.chunk_doc_idx];
            let mut total_needed_positions = self.chunk_counts[self.chunk_doc_idx] as usize;
            self.chunk_doc_idx += 1;

            while self.chunk_doc_idx < self.chunk_tids.len()
                && self.chunk_tids[self.chunk_doc_idx] == tid
            {
                total_needed_positions += self.chunk_counts[self.chunk_doc_idx] as usize;
                self.chunk_doc_idx += 1;
            }

            let mut found = false;
            for _ in 0..total_needed_positions {
                let delta = self.next_pos_delta()?;
                self.pos_acc = self.pos_acc.wrapping_add(delta);
                let flag = Self::next_flag(&mut self.flags)?;
                if self.pos_acc == target_pos
                    && (!case_sensitive || (flag & 0b111) == (pattern_flag & 0b111))
                {
                    found = true;
                }
            }

            self.current = None;
            return Ok(Some((tid, found)));
        }
    }
}
