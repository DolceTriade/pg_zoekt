use std::collections::BTreeMap;
use std::io::Write;

use super::pgbuffer::BlockBuffer;
use anyhow::Context;
use delta_encoding::DeltaEncoderExt;
use pgrx::prelude::*;
use zerocopy::IntoBytes;

pub struct Encoder;

impl Encoder {
    pub fn encode_trgms(
        rel: pg_sys::Relation,
        trgms: &BTreeMap<u32, BTreeMap<crate::storage::ItemPointer, Vec<crate::trgm::Occurance>>>,
    ) -> anyhow::Result<Vec<super::Segment>> {
        if trgms.is_empty() {
            return Ok(Vec::new());
        }
        let mut remaining_trgms = trgms.len();
        let mut iter = trgms.iter();
        info!("Encoding {remaining_trgms} trigram entries");
        let mut doc_count = 0;
        let mut occ_count = 0;
        let mut byte_count = 0;
        let mut segment = super::Segment { block: 0, size: 0 };
        let mut leaf_pointers: Vec<super::BlockPointer> = Vec::new();

        let mut p = PageWriter::new(rel, super::pgbuffer::SPECIAL_SIZE);
        let mut interrupt_counter: u32 = 0;

        while remaining_trgms > 0 {
            interrupt_counter = interrupt_counter.wrapping_add(1);
            if (interrupt_counter & 0x3ff) == 0 {
                pg_sys::check_for_interrupts!();
            }
            let mut leaf = super::allocate_block(rel);
            let leaf_block = leaf.block_number();
            const BH_SIZE: usize = std::mem::size_of::<super::BlockHeader>();
            let bh = leaf
                .as_struct_mut::<super::BlockHeader>(0)
                .context("does this work")?;
            bh.magic = super::BLOCK_MAGIC;
            bh.level = 0;
            const ENTRY_SIZE: usize = std::mem::size_of::<super::IndexEntry>();
            let num_entries =
                ((super::pgbuffer::SPECIAL_SIZE - BH_SIZE) / ENTRY_SIZE).min(remaining_trgms);
            bh.num_entries = num_entries as u32;
            let entries = leaf
                .as_struct_with_elems_mut::<super::IndexList>(BH_SIZE, num_entries)
                .context("could not parse entries")?;
            let mut leaf_min_trigram: Option<u32> = None;
            for i in 0..num_entries {
                interrupt_counter = interrupt_counter.wrapping_add(1);
                if (interrupt_counter & 0x3ff) == 0 {
                    pg_sys::check_for_interrupts!();
                }
                let Some((key, val)) = iter.next() else {
                    anyhow::bail!("Unexpected end of iterator");
                };
                if leaf_min_trigram.is_none() {
                    leaf_min_trigram = Some(*key);
                }
                let idx = &mut entries.entries[i];
                idx.trigram = *key;
                idx.block = 0;
                idx.offset = 0;
                idx.data_length = 0;
                idx.frequency = val.len() as u32;
                let mut b = CompressedBatchBuilder::new();
                let max_chunk_size =
                    super::pgbuffer::SPECIAL_SIZE - std::mem::size_of::<super::PostingPageHeader>();
                let mut first_chunk = false;
                let mut compressed = Vec::<u8>::new();

                let mut flush_chunk = |builder: &mut CompressedBatchBuilder,
                                       idx: &mut super::IndexEntry,
                                       first_chunk: &mut bool,
                                       byte_count: &mut usize|
                 -> anyhow::Result<()> {
                    if builder.is_empty() {
                        return Ok(());
                    }
                    builder.compress_into(&mut compressed);
                    if compressed.len() > max_chunk_size {
                        anyhow::bail!(
                            "chunk size {} exceeds page capacity {}",
                            compressed.len(),
                            max_chunk_size
                        );
                    }
                    let loc = p.start_chunk(compressed.len());
                    p.write_all(&compressed).expect("Write to succeed");
                    if !*first_chunk {
                        idx.block = loc.block_number;
                        idx.offset = loc.offset as u16;
                        *first_chunk = true;
                    }
                    idx.data_length = idx
                        .data_length
                        .checked_add(compressed.len() as u32)
                        .expect("overflow on data length");
                    *byte_count += compressed.len();
                    builder.reset();
                    Ok(())
                };

                for (doc, occs) in val {
                    interrupt_counter = interrupt_counter.wrapping_add(1);
                    if (interrupt_counter & 0x3ff) == 0 {
                        pg_sys::check_for_interrupts!();
                    }
                    doc_count += 1;
                    occ_count += occs.len();

                    if occs.is_empty() {
                        continue;
                    }

                    let mut start = 0usize;
                    while start < occs.len() {
                        if b.num_docs() >= u8::MAX as usize {
                            flush_chunk(&mut b, idx, &mut first_chunk, &mut byte_count)?;
                            continue;
                        }

                        let remaining = occs.len() - start;
                        let can_take = b.max_positions_fit(max_chunk_size).min(remaining);
                        if can_take == 0 {
                            if b.is_empty() {
                                anyhow::bail!(
                                    "single doc chunk size {} exceeds page capacity {}",
                                    occs.len(),
                                    max_chunk_size
                                );
                            }
                            flush_chunk(&mut b, idx, &mut first_chunk, &mut byte_count)?;
                            continue;
                        }

                        b.add(*doc, &occs[start..start + can_take]);
                        start += can_take;
                    }
                }
                flush_chunk(&mut b, idx, &mut first_chunk, &mut byte_count)?;
            }
            remaining_trgms -= num_entries;
            let min_trigram = leaf_min_trigram.context("leaf should have min trigram")?;
            leaf_pointers.push(super::BlockPointer {
                min_trigram,
                block: leaf_block,
            });

            // Start with leaf node.
            // Write until full.
            // Allocate meta page
            // update meta page
            // start new leaf
            // ...

            // For each leaf node:
            // Write index entry
            // Start writing compressed blocks of 128 entries
        }
        info!("Encoded {doc_count} docs, {occ_count} occs and {byte_count} bytes");
        segment.size = byte_count as u64;
        segment.block = build_segment_root(rel, &leaf_pointers)?;
        Ok(vec![segment])
    }
}

pub(crate) fn build_segment_root(
    rel: pg_sys::Relation,
    leaves: &[super::BlockPointer],
) -> anyhow::Result<u32> {
    if leaves.is_empty() {
        anyhow::bail!("no leaves");
    }
    if leaves.len() == 1 {
        return Ok(leaves[0].block);
    }

    const BH_SIZE: usize = std::mem::size_of::<super::BlockHeader>();
    const PTR_SIZE: usize = std::mem::size_of::<super::BlockPointer>();
    let per_page = (super::pgbuffer::SPECIAL_SIZE - BH_SIZE) / PTR_SIZE;
    if per_page == 0 {
        anyhow::bail!("block pointers do not fit on a page");
    }

    let mut level: u8 = 1;
    let mut current: Vec<super::BlockPointer> = leaves.to_vec();
    while current.len() > 1 {
        let mut next = Vec::new();
        for chunk in current.chunks(per_page) {
            let mut page = super::allocate_block(rel);
            let block_no = page.block_number();
            let bh = page
                .as_struct_mut::<super::BlockHeader>(0)
                .context("block header")?;
            bh.magic = super::BLOCK_MAGIC;
            bh.level = level;
            bh.num_entries = chunk.len() as u32;
            let ptrs = page
                .as_struct_with_elems_mut::<super::BlockPointerList>(BH_SIZE, chunk.len())
                .context("pointer list")?;
            for (idx, p) in chunk.iter().enumerate() {
                ptrs.entries[idx] = *p;
            }
            next.push(super::BlockPointer {
                min_trigram: chunk[0].min_trigram,
                block: block_no,
            });
        }
        current = next;
        level = level.saturating_add(1);
    }
    Ok(current[0].block)
}

#[derive(Debug)]
pub(super) struct CompressedBatchBuilder {
    blks: Vec<u32>,
    offs: Vec<u16>,

    counts: Vec<u32>,

    positions: Vec<u32>,

    flags: Vec<u8>,
    // Conservative upper bound used for chunk-splitting decisions.
    // We intentionally overestimate to keep the hot path linear.
    worst_size: usize,

    encoder: StreamVByteEncoder,
    scratch: Vec<u32>,
    payload: Vec<u8>,
}

fn encode_section_into<F>(
    out: &mut Vec<u8>,
    encoder: &mut StreamVByteEncoder,
    scratch: &mut Vec<u32>,
    fill: F,
) -> usize
where
    F: FnOnce(&mut Vec<u32>),
{
    scratch.clear();
    fill(scratch);
    encoder.clear();
    encoder.append(scratch.as_slice());
    let len = encoder.len();
    encoder.append_into(out);
    len
}

impl CompressedBatchBuilder {
    // Worst-case accounting (intentionally conservative):
    // - StreamVByte worst-case ~5 bytes per integer (data+control).
    // - Flags RLE can be pathological; budget 2 bytes per flag.
    const DOC_OVERHEAD_WORST: usize = 5 * 3;
    const PER_POS_WORST: usize = 5 + 2;

    pub fn new() -> Self {
        Self {
            blks: Vec::new(),
            offs: Vec::new(),
            counts: Vec::new(),
            positions: Vec::new(),
            flags: Vec::new(),
            worst_size: std::mem::size_of::<super::CompressedBlockHeader>(),
            encoder: StreamVByteEncoder::new(),
            scratch: Vec::new(),
            payload: Vec::new(),
        }
    }

    pub fn num_docs(&self) -> usize {
        self.blks.len()
    }

    pub fn max_positions_fit(&self, max_chunk_size: usize) -> usize {
        if self.num_docs() >= u8::MAX as usize {
            return 0;
        }
        let remaining = max_chunk_size.saturating_sub(self.worst_size);
        if remaining <= Self::DOC_OVERHEAD_WORST {
            return 0;
        }
        (remaining - Self::DOC_OVERHEAD_WORST) / Self::PER_POS_WORST
    }

    pub fn add(&mut self, doc: super::ItemPointer, occs: &[crate::trgm::Occurance]) {
        self.worst_size += Self::DOC_OVERHEAD_WORST + Self::PER_POS_WORST * occs.len();

        self.blks.push(doc.block_number);

        self.offs.push(doc.offset);

        self.counts.push(occs.len() as u32);

        self.positions.reserve(occs.len());
        self.flags.reserve(occs.len());
        for occ in occs {
            self.positions.push(occ.position());
            self.flags.push(occ.flags());
        }
    }

    pub fn compress_into(&mut self, dst: &mut Vec<u8>) {
        let num_docs = self.blks.len();
        let header_size = std::mem::size_of::<super::CompressedBlockHeader>();
        let estimated_ints = self.positions.len() + self.blks.len() * 3;
        let payload = &mut self.payload;
        payload.clear();
        payload.reserve((estimated_ints * 5) + self.flags.len());

        let encoder = &mut self.encoder;
        let scratch = &mut self.scratch;
        scratch.reserve(
            self.positions
                .len()
                .max(self.blks.len())
                .max(self.counts.len())
                .max(self.offs.len()),
        );

        let blks = &self.blks;
        let offs = &self.offs;
        let counts = &self.counts;
        let positions = &self.positions;

        let blks_len = encode_section_into(payload, encoder, scratch, |tmp| {
            tmp.extend(blks.iter().copied().deltas())
        });
        let offs_len = encode_section_into(payload, encoder, scratch, |tmp| {
            tmp.extend(offs.iter().map(|v| *v as u32))
        });
        let counts_len = encode_section_into(payload, encoder, scratch, |tmp| {
            tmp.extend(counts.iter().copied())
        });
        let positions_len = encode_section_into(payload, encoder, scratch, |tmp| {
            tmp.extend(positions.iter().copied().deltas())
        });

        let flags_len = {
            let mut b = bitfield_rle::encode(&self.flags);
            let l = b.len();
            payload.append(&mut b);
            l
        };
        let hdr = super::CompressedBlockHeader {
            num_docs: num_docs as u8,
            docs_blk_len: blks_len as u16,
            docs_off_len: offs_len as u16,
            counts_len: counts_len as u16,
            pos_len: positions_len as u16,
            flags_len: flags_len as u16,
        };
        dst.clear();
        dst.reserve(header_size + payload.len());
        hdr.write_to_io(&mut *dst).expect("not fail");
        dst.extend_from_slice(payload);
    }

    pub fn is_empty(&self) -> bool {
        self.blks.is_empty()
    }

    pub fn reset(&mut self) {
        self.blks.clear();
        self.offs.clear();
        self.counts.clear();
        self.positions.clear();
        self.flags.clear();
        self.worst_size = std::mem::size_of::<super::CompressedBlockHeader>();

        // Avoid retaining huge allocations after an outlier trigram/doc.
        const MAX_RETAIN_BYTES: usize = 256 * 1024;
        fn maybe_shrink<T>(v: &mut Vec<T>, max_retain_bytes: usize) {
            let retained = v.capacity().saturating_mul(std::mem::size_of::<T>());
            if retained > max_retain_bytes {
                v.shrink_to_fit();
            }
        }

        maybe_shrink(&mut self.positions, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.flags, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.blks, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.offs, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.counts, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.scratch, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.payload, MAX_RETAIN_BYTES);
        maybe_shrink(&mut self.encoder.buffer, MAX_RETAIN_BYTES);
    }

    // Encoding helpers live outside the impl (see `encode_section_into`) so we
    // can split borrows between the input vectors and the reusable scratch/encoder.
}

#[derive(Debug)]
pub(super) struct StreamVByteEncoder {
    buffer: Vec<u8>,
}

impl StreamVByteEncoder {
    pub fn new() -> Self {
        Self {
            // 128 u32s + control bytes.
            buffer: Vec::with_capacity(512),
        }
    }

    /// Appends a list of integers to the stream.
    /// Returns the number of bytes written.
    pub fn append(&mut self, numbers: &[u32]) -> usize {
        if numbers.is_empty() {
            return 0;
        }

        // 1. Calculate worst-case size (N * 5 bytes for u32)
        // StreamVByte needs 4 bytes data + 1 byte control per int
        let required_cap = numbers.len() * 5;
        let start_len = self.buffer.len();

        // 2. Reserve space (avoid reallocations during encode)
        self.buffer.reserve(required_cap);

        // 3. Extend the vector with zeros to act as the scratch buffer
        // (This is the awkward part of the crate: it demands a mutable slice)
        self.buffer.resize(start_len + required_cap, 0);

        // 4. Encode directly into the tail of the vector
        let bytes_written = stream_vbyte::encode::encode::<stream_vbyte::x86::Sse41>(
            numbers,
            &mut self.buffer[start_len..],
        );

        // 5. Truncate back to the actual used size
        self.buffer.truncate(start_len + bytes_written);

        bytes_written
    }

    pub fn append_into(&mut self, out: &mut Vec<u8>) {
        out.append(&mut self.buffer);
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Reset for reuse (avoids dropping the allocation)
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

const POSTING_PAGE_HEADER_SIZE: usize = std::mem::size_of::<super::PostingPageHeader>();

pub(super) struct PageWriter {
    rel: pg_sys::Relation,
    buff: Option<BlockBuffer>,
    size: usize,
    pos: usize,
}

impl PageWriter {
    pub fn new(rel: pg_sys::Relation, size: usize) -> Self {
        let mut writer = Self {
            rel,
            buff: None,
            size,
            pos: 0,
        };
        writer.ensure_page_available();
        writer
    }

    fn ensure_page_available(&mut self) {
        if self.buff.is_none() {
            self.allocate_page();
        } else if self.pos >= self.size {
            self.allocate_next_page();
        }
    }

    fn ensure_page_available_for(&mut self, required: usize) {
        let max_chunk = self.size - POSTING_PAGE_HEADER_SIZE;
        if required > max_chunk {
            panic!("chunk size {required} exceeds page capacity {max_chunk}");
        }
        loop {
            if self.buff.is_none() {
                self.allocate_page();
                continue;
            }
            let available = self.size - self.pos;
            if available >= required {
                break;
            }
            self.allocate_next_page();
        }
    }

    fn allocate_page(&mut self) {
        let mut page = super::allocate_block(self.rel);
        let header = page
            .as_struct_mut::<super::PostingPageHeader>(0)
            .expect("header should fit");
        header.magic = super::POSTING_PAGE_MAGIC;
        header.next_block = pg_sys::InvalidBlockNumber;
        header.next_offset = POSTING_PAGE_HEADER_SIZE as u16;
        header.free = POSTING_PAGE_HEADER_SIZE as u16;
        self.buff = Some(page);
        self.pos = POSTING_PAGE_HEADER_SIZE;
    }

    fn allocate_next_page(&mut self) {
        let mut next_page = super::allocate_block(self.rel);
        let next_block = next_page.block_number();
        if let Some(mut old) = self.buff.take() {
            let header = old
                .as_struct_mut::<super::PostingPageHeader>(0)
                .expect("header should fit");
            header.next_block = next_block;
            header.next_offset = POSTING_PAGE_HEADER_SIZE as u16;
            header.free = self.pos as u16;
        }
        let header = next_page
            .as_struct_mut::<super::PostingPageHeader>(0)
            .expect("header should fit");
        header.magic = super::POSTING_PAGE_MAGIC;
        header.next_block = pg_sys::InvalidBlockNumber;
        header.next_offset = POSTING_PAGE_HEADER_SIZE as u16;
        header.free = POSTING_PAGE_HEADER_SIZE as u16;
        self.buff = Some(next_page);
        self.pos = POSTING_PAGE_HEADER_SIZE;
    }

    fn location(&self) -> Option<super::ItemPointer> {
        self.buff.as_ref().map(|b| super::ItemPointer {
            block_number: b.block_number(),
            offset: self.pos as u16,
        })
    }

    pub fn start_chunk(&mut self, size: usize) -> super::ItemPointer {
        self.ensure_page_available_for(size);
        self.location().expect("location should exist")
    }
}

impl Write for PageWriter {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while !buf.is_empty() {
            self.ensure_page_available();
            let available = self.size - self.pos;
            if available == 0 {
                self.allocate_next_page();
                continue;
            }
            let to_write = available.min(buf.len());
            if let Some(b) = self.buff.as_mut() {
                unsafe {
                    let p = b.as_ptr_mut().add(self.pos);
                    std::ptr::copy(buf.as_ptr(), p as *mut u8, to_write);
                }
                self.pos += to_write;
                let header = b
                    .as_struct_mut::<super::PostingPageHeader>(0)
                    .expect("header should fit");
                header.free = self.pos as u16;
            }
            written += to_write;
            buf = &buf[to_write..];
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        _ = self.buff.take();
        self.pos = 0;
        Ok(())
    }
}
