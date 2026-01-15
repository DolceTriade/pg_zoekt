use anyhow::Context;
use pgrx::prelude::*;
use zerocopy::TryFromBytes;

use std::io::Write;

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

    blk_nums_buf: Vec<u32>,
    offs_buf: Vec<u32>,
    counts_buf: Vec<u32>,

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
        let (block, offset, data_length) = entry_fields(entry);
        let nblocks = unsafe {
            pg_sys::RelationGetNumberOfBlocksInFork(rel, pg_sys::ForkNumber::MAIN_FORKNUM)
        };
        if block == pg_sys::InvalidBlockNumber || block >= nblocks {
            anyhow::bail!(
                "posting block out of range: {} (nblocks={})",
                block,
                nblocks
            );
        }
        let buf = BlockBuffer::acquire(rel, block)?;
        let header_copy = {
            let header = buf
                .as_struct::<super::PostingPageHeader>(0)
                .context("posting page header")?;
            *header
        };
        if header_copy.magic != super::POSTING_PAGE_MAGIC {
            anyhow::bail!("invalid posting page magic");
        }
        let start = offset as usize;
        if start < header_copy.next_offset as usize || start > header_copy.free as usize {
            anyhow::bail!("offset out of bounds");
        }
        Ok(Self {
            rel,
            buf,
            header: header_copy,
            cursor: start,
            page_free: header_copy.free as usize,
            remaining: data_length as usize,
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
        let nblocks = unsafe {
            pg_sys::RelationGetNumberOfBlocksInFork(self.rel, pg_sys::ForkNumber::MAIN_FORKNUM)
        };
        if next_block >= nblocks {
            anyhow::bail!(
                "posting next block out of range: {} (nblocks={})",
                next_block,
                nblocks
            );
        }
        let next_buf = BlockBuffer::acquire(self.rel, next_block)?;
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

    fn copy_to<W: Write>(&mut self, mut len: usize, out: &mut W) -> anyhow::Result<()> {
        if len > self.remaining {
            anyhow::bail!("posting copy exceeds declared length");
        }
        while len > 0 {
            let available = self.page_free.saturating_sub(self.cursor);
            if available == 0 {
                self.advance_page()?;
                continue;
            }
            let take = available.min(len);
            let page = self.buf.as_ref();
            let end = self.cursor + take;
            out.write_all(&page[self.cursor..end])
                .context("write posting bytes")?;
            self.cursor = end;
            self.remaining -= take;
            len -= take;
        }
        Ok(())
    }
}

fn entry_fields(entry: &IndexEntry) -> (u32, u16, u32) {
    let block = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.block)) };
    let offset = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.offset)) };
    let data_length = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.data_length)) };
    (block, offset, data_length)
}

pub(crate) unsafe fn copy_posting_bytes<W: Write>(
    rel: pg_sys::Relation,
    entry: &IndexEntry,
    len: usize,
    out: &mut W,
) -> anyhow::Result<()> {
    let mut reader = unsafe { PostingReader::new(rel, entry) }?;
    reader.copy_to(len, out)
}

#[cfg(test)]
mod tests {
    use super::entry_fields;
    use crate::storage::IndexEntry;

    #[test]
    fn entry_fields_reads_unaligned() {
        let entry = IndexEntry {
            trigram: 0xdeadbeef,
            block: 42,
            offset: 17,
            data_length: 99,
            frequency: 0,
        };
        let mut buf = vec![0u8; std::mem::size_of::<IndexEntry>() + 1];
        let ptr = unsafe { buf.as_mut_ptr().add(1) as *mut IndexEntry };
        unsafe {
            std::ptr::write_unaligned(ptr, entry);
        }
        let entry_ref = unsafe { &*ptr };
        let (block, offset, data_length) = entry_fields(entry_ref);
        assert_eq!(block, 42);
        assert_eq!(offset, 17);
        assert_eq!(data_length, 99);
    }
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
            blk_nums_buf: Vec::new(),
            offs_buf: Vec::new(),
            counts_buf: Vec::new(),
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
        self.chunk_tids.reserve(num_docs);
        self.chunk_counts.reserve(num_docs);

        let blk_bytes = self.reader.take_slice(hdr.docs_blk_len as usize)?;
        self.blk_nums_buf.clear();
        self.blk_nums_buf.resize(num_docs, 0u32);
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            blk_bytes,
            num_docs,
            &mut self.blk_nums_buf,
        );
        let mut acc = 0u32;
        for blk in self.blk_nums_buf.iter_mut() {
            acc = acc.wrapping_add(*blk);
            *blk = acc;
        }

        let off_bytes = self.reader.take_slice(hdr.docs_off_len as usize)?;
        self.offs_buf.clear();
        self.offs_buf.resize(num_docs, 0u32);
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            off_bytes,
            num_docs,
            &mut self.offs_buf,
        );

        let count_bytes = self.reader.take_slice(hdr.counts_len as usize)?;
        self.counts_buf.clear();
        self.counts_buf.resize(num_docs, 0u32);
        stream_vbyte::decode::decode::<stream_vbyte::x86::Ssse3>(
            count_bytes,
            num_docs,
            &mut self.counts_buf,
        );
        let total_positions = self
            .counts_buf
            .iter()
            .copied()
            .map(|c| c as usize)
            .sum::<usize>();

        self.chunk_counts.extend(self.counts_buf.iter().copied());
        for (blk, off) in self
            .blk_nums_buf
            .iter()
            .copied()
            .zip(self.offs_buf.iter().copied())
        {
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
            }

            let tid = self.chunk_tids[self.chunk_doc_idx];
            let mut positions = if let Some(doc) = self.current.take() {
                let mut p = doc.positions;
                p.clear();
                p
            } else {
                Vec::with_capacity(4)
            };

            loop {
                let mut total_needed_positions = self.chunk_counts[self.chunk_doc_idx] as usize;
                self.chunk_doc_idx += 1;

                while self.chunk_doc_idx < self.chunk_tids.len()
                    && self.chunk_tids[self.chunk_doc_idx] == tid
                {
                    total_needed_positions += self.chunk_counts[self.chunk_doc_idx] as usize;
                    self.chunk_doc_idx += 1;
                }

                positions.reserve(total_needed_positions);
                for _ in 0..total_needed_positions {
                    let delta = self.next_pos_delta()?;
                    self.pos_acc = self.pos_acc.wrapping_add(delta);
                    let flag = Self::next_flag(&mut self.flags)?;
                    positions.push((self.pos_acc, flag));
                }

                if self.chunk_doc_idx >= self.chunk_tids.len() {
                    if !self.load_next_chunk()? {
                        break;
                    }
                    if self.chunk_tids.first().copied() == Some(tid) {
                        continue;
                    }
                }

                break;
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
            }

            let tid = self.chunk_tids[self.chunk_doc_idx];
            let mut found = false;

            loop {
                let mut total_needed_positions = self.chunk_counts[self.chunk_doc_idx] as usize;
                self.chunk_doc_idx += 1;

                while self.chunk_doc_idx < self.chunk_tids.len()
                    && self.chunk_tids[self.chunk_doc_idx] == tid
                {
                    total_needed_positions += self.chunk_counts[self.chunk_doc_idx] as usize;
                    self.chunk_doc_idx += 1;
                }

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

                if self.chunk_doc_idx >= self.chunk_tids.len() {
                    if !self.load_next_chunk()? {
                        self.current = None;
                        return Ok(Some((tid, found)));
                    }
                    if self.chunk_tids.first().copied() == Some(tid) {
                        continue;
                    }
                }

                break;
            }

            self.current = None;
            return Ok(Some((tid, found)));
        }
    }
}
