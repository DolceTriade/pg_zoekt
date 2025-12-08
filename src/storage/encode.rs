use std::io::{Read, Write};

use super::pgbuffer::BlockBuffer;
use anyhow::Context;
use delta_encoding::DeltaEncoderExt;
use pgrx::prelude::*;
use zerocopy::IntoBytes;

pub struct Encoder<'a> {
    collector: &'a crate::trgm::Collector,
}

impl<'a> Encoder<'a> {
    pub fn new(collector: &'a crate::trgm::Collector) -> Self {
        Self { collector }
    }

    pub fn encode(&self, rel: pg_sys::Relation) -> anyhow::Result<super::Segment> {
        let trgms = self.collector.trgms();
        let meta1: Option<BlockBuffer> = None;
        let data: Option<BlockBuffer> = None;
        let mut remaining_trgms = trgms.len();
        let mut iter = trgms.iter();
        info!("Encoding {remaining_trgms}");
        let mut doc_count = 0;
        let mut occ_count = 0;
        let mut byte_count = 0;
        let mut segment = super::Segment { block: 0, size: 0 };

        let mut p = PageWriter::new(rel, super::pgbuffer::SPECIAL_SIZE);

        while remaining_trgms > 0 {
            let mut leaf = BlockBuffer::allocate(rel);
            segment.block = leaf.block_number();
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
            info!("Num entries is {num_entries}");
            let entries = leaf
                .as_struct_with_elems_mut::<super::IndexList>(BH_SIZE, num_entries)
                .context("could not parse entries")?;
            for i in 0..num_entries {
                let Some((key, val)) = iter.next() else {
                    anyhow::bail!("Unexpected end of iterator");
                };
                let idx = &mut entries.entries[i];
                idx.trigram = *key;
                idx.block = 0;
                idx.offset = 0;
                idx.frequency = val.len() as u32;
                let mut b = CompressedBatchBuilder::new();
                for (doc, occs) in val {
                    b.add(*doc, &occs);
                    doc_count += 1;
                    occ_count += occs.len();
                }
                let buf = b.compress();
                p.write(&buf).expect("Write to succeed");
                let loc = p.location().expect("have a location");
                idx.block = loc.block_number;
                idx.offset = loc.offset - (buf.len() as u16);
                byte_count += buf.len();
            }
            remaining_trgms -= num_entries;

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
        Ok(segment)
    }
}

#[derive(Debug, Default)]
struct CompressedBatchBuilder {
    blks: Vec<u32>,
    offs: Vec<u16>,

    counts: Vec<u16>,

    positions: Vec<u32>,

    flags: Vec<u8>,
}

impl CompressedBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, doc: super::ItemPointer, occs: &[crate::trgm::Occurance]) {
        self.blks.push(doc.block_number);
        self.offs.push(doc.offset);
        self.counts.push(occs.len() as u16);
        self.positions.reserve(occs.len());
        self.flags.reserve(occs.len());
        for occ in occs {
            self.positions.push(occ.position());
            self.flags.push(occ.flags());
        }
    }

    pub fn compress(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        let num_docs = self.blks.len();
        let blks_len = self.encode_section(&mut out, |enc| {
            let delta: Vec<u32> = self.blks.iter().copied().deltas().collect();
            enc.append(&delta);
        });
        let offs_len = self.encode_section(&mut out, |enc| {
            let tmp: Vec<u32> = self.offs.iter().map(|v| *v as u32).collect();
            enc.append(tmp.as_slice());
        });
        let counts_len = self.encode_section(&mut out, |enc| {
            let tmp: Vec<u32> = self.counts.iter().map(|v| *v as u32).collect();
            enc.append(tmp.as_slice());
        });
        let positions_len = self.encode_section(&mut out, |enc| {
            let delta: Vec<u32> = self.positions.iter().copied().deltas().collect();
            enc.append(&delta);
        });
        let flags_len = {
            let mut b = bitfield_rle::encode(&self.flags);
            let l = b.len();
            out.append(&mut b);
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
        let mut c = std::io::Cursor::new(Vec::new());
        hdr.write_to_io(&mut c).expect("not fail");
        c.write(&out).expect("not fail");
        c.into_inner()
    }

    fn encode_section<F>(&self, out: &mut Vec<u8>, fill: F) -> usize
    where
        F: FnOnce(&mut VByteEncoder),
    {
        let mut enc = VByteEncoder::new();
        fill(&mut enc);
        let mut b = enc.into_inner();
        let len = b.len();
        out.append(&mut b);
        len
    }
}

pub struct VByteEncoder {
    buffer: Vec<u8>,
}

impl VByteEncoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
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

    /// Finish and return the underlying buffer
    pub fn into_inner(self) -> Vec<u8> {
        self.buffer
    }

    /// Reset for reuse (avoids dropping the allocation)
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

struct PageWriter {
    rel: pg_sys::Relation,
    buff: Option<BlockBuffer>,
    size: usize,
    pos: usize,
}

impl PageWriter {
    pub fn new(rel: pg_sys::Relation, size: usize) -> Self {
        Self {
            rel,
            buff: Some(BlockBuffer::allocate(rel)),
            size,
            pos: 0,
        }
    }

    pub fn location(&self) -> Option<super::ItemPointer> {
        match self.buff.as_ref() {
            Some(b) => Some(super::ItemPointer {
                block_number: b.block_number(),
                offset: self.pos as u16,
            }),
            None => None,
        }
    }
}

impl Write for PageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() >= self.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "cannot fit in buffer",
            ));
        }
        if self.buff.is_none() || self.pos + buf.len() >= self.size {
            _ = self.buff.replace(BlockBuffer::allocate(self.rel));
            self.pos = 0;
        }
        if let Some(b) = self.buff.as_mut() {
            unsafe {
                let p = b.as_ptr_mut().add(self.pos);
                std::ptr::copy(buf.as_ptr(), p as *mut u8, buf.len());
            }
        }
        self.pos += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // drop to force flush
        _ = self.buff.take();
        Ok(())
    }
}
