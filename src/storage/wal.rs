use std::collections::BTreeMap;
use std::ptr;

use super::{
    ItemPointer, ROOT_MAGIC, RootBlockList, Segment, WAL_BUCKET_MAGIC, WAL_MAGIC, WALBucket,
    WALEntry, WALHeader, WALTrigram, encode, pgbuffer::BlockBuffer,
};
use crate::trgm::{Collector, Occurance};
use anyhow::{Context, Result, bail};
use pgrx::pg_sys;

const PAGE_HEADER_SIZE: usize = std::mem::size_of::<WALBucket>();
const WAL_TRIGRAM_SIZE: usize = std::mem::size_of::<WALTrigram>();
const WAL_ENTRY_SIZE: usize = std::mem::size_of::<WALEntry>();
const POSITION_SIZE: usize = std::mem::size_of::<u32>();

const INVALID_BLOCK: u32 = pg_sys::InvalidBlockNumber;

fn page_capacity() -> usize {
    super::pgbuffer::SPECIAL_SIZE - PAGE_HEADER_SIZE
}

fn record_size(num_positions: usize) -> usize {
    WAL_TRIGRAM_SIZE
        + WAL_ENTRY_SIZE
        + num_positions
            .checked_mul(POSITION_SIZE)
            .expect("positions size overflow")
}

fn read_struct<T: Copy>(bytes: &[u8]) -> T {
    debug_assert!(bytes.len() >= std::mem::size_of::<T>());
    unsafe { ptr::read_unaligned(bytes.as_ptr() as *const T) }
}

fn write_struct<T: Copy>(bytes: &mut [u8], value: T) {
    debug_assert!(bytes.len() >= std::mem::size_of::<T>());
    unsafe {
        ptr::write_unaligned(bytes.as_mut_ptr() as *mut T, value);
    }
}

fn load_wal_block(rel: pg_sys::Relation, root_block: u32) -> Result<u32> {
    let root = BlockBuffer::acquire(rel, root_block);
    let header = root.as_struct::<RootBlockList>(0).context("root header")?;
    if header.magic != ROOT_MAGIC {
        bail!("invalid root magic while accessing WAL");
    }
    if header.wal_block == INVALID_BLOCK {
        bail!("index WAL root is missing");
    }
    Ok(header.wal_block)
}

pub fn append_document(
    rel: pg_sys::Relation,
    root_block: u32,
    ctid: ItemPointer,
    postings: &BTreeMap<u32, Vec<Occurance>>,
    flush_threshold: usize,
) -> Result<Vec<Segment>> {
    if postings.is_empty() {
        return Ok(Vec::new());
    }
    let wal_block = load_wal_block(rel, root_block)?;
    let writer = WalWriter::new(rel, wal_block, flush_threshold)?;

    // Check global threshold (approximate)
    let sealed = if writer.should_flush() {
        writer.flush_all()?
    } else {
        Vec::new()
    };

    for (trigram, occs) in postings {
        pg_sys::check_for_interrupts!();
        if occs.is_empty() {
            continue;
        }
        writer.append_trigram(*trigram, ctid, occs)?;
    }
    Ok(sealed)
}

pub fn flush_pending(rel: pg_sys::Relation, root_block: u32) -> Result<Vec<Segment>> {
    let wal_block = load_wal_block(rel, root_block)?;
    let writer = WalWriter::new(rel, wal_block, 0)?;
    writer.flush_all()
}

struct WalWriter {
    rel: pg_sys::Relation,
    wal_block: u32,
    flush_threshold: usize,
}

impl WalWriter {
    fn new(rel: pg_sys::Relation, wal_block: u32, flush_threshold: usize) -> Result<Self> {
        let wal_page = BlockBuffer::acquire(rel, wal_block);
        let header = wal_page.as_struct::<WALHeader>(0).context("wal header")?;
        if header.magic != WAL_MAGIC {
            bail!("invalid WAL magic");
        }
        drop(wal_page);
        let flush_threshold = flush_threshold.max(page_capacity());
        Ok(Self {
            rel,
            wal_block,
            flush_threshold,
        })
    }

    fn should_flush(&self) -> bool {
        let wal_page = BlockBuffer::acquire(self.rel, self.wal_block);
        let header = match wal_page.as_struct::<WALHeader>(0) {
            Ok(h) => h,
            Err(_) => return false,
        };
        header.bytes_used as usize >= self.flush_threshold
    }

    fn append_trigram(&self, trigram: u32, ctid: ItemPointer, occs: &[Occurance]) -> Result<()> {
        let needed = record_size(occs.len());
        if needed > page_capacity() {
            bail!(
                "document payload ({} bytes) exceeds WAL page capacity {}",
                needed,
                page_capacity()
            );
        }

        loop {
            // Lock WAL header to get/set tail
            let mut wal_buf = BlockBuffer::aquire_mut(self.rel, self.wal_block);
            let wal = wal_buf
                .as_struct_mut::<WALHeader>(0)
                .context("wal header")?;

            let tail_blk = wal.tail_block;

            if tail_blk == INVALID_BLOCK {
                // Initialize first block
                let new_block = self.allocate_page(&mut wal.free_head)?;
                wal.head_block = new_block;
                wal.tail_block = new_block;
                // Retry loop to write to this new block
                continue;
            }

            // Attempt to write to tail
            // We must release WAL header lock before acquiring tail page lock to avoid deadlocks?
            // Actually, usually we hold header lock if we are modifying the chain structure.
            // But we want to allow concurrent writes if we had buckets.
            // Here, it's linear. Single mutex (WAL Header) is fine for simplicity and correctness.
            // However, strictly speaking, we should try to lock tail, check space, write.

            // Optimization: Don't hold WAL header lock while writing data.
            // But we need to know `tail_block` hasn't changed.
            // Let's just hold the lock for now. It's "Staging", batch inserts are likely single-threaded per connection.
            // If parallel index build is used, we might need finer locking.
            // Given "Simple Linear Log", let's keep it simple.

            let mut page = BlockBuffer::aquire_mut(self.rel, tail_blk);
            let free = {
                let header = page.as_struct::<WALBucket>(0).context("wal page header")?;
                if header.magic != WAL_BUCKET_MAGIC {
                    bail!("corrupt WAL page");
                }
                header.free as usize
            };

            if free < needed {
                // Tail full. Allocate new.
                drop(page);
                let new_block = self.allocate_page(&mut wal.free_head)?;

                // Link old tail to new
                let mut old_tail = BlockBuffer::aquire_mut(self.rel, tail_blk);
                let old_header = old_tail
                    .as_struct_mut::<WALBucket>(0)
                    .context("wal page header")?;
                old_header.next_block = new_block;
                drop(old_tail);

                wal.tail_block = new_block;
                continue;
            }

            self.write_record(&mut page, trigram, ctid, occs)?;
            {
                let header = page
                    .as_struct_mut::<WALBucket>(0)
                    .context("wal page header")?;
                let needed_u16: u16 = needed.try_into().unwrap();
                header.free = header.free.checked_sub(needed_u16).expect("underflow");
            }
            wal.bytes_used = wal.bytes_used.saturating_add(needed as u32);

            return Ok(());
        }
    }

    fn write_record(
        &self,
        page: &mut BlockBuffer,
        trigram: u32,
        ctid: ItemPointer,
        occs: &[Occurance],
    ) -> Result<()> {
        let payload_start = PAGE_HEADER_SIZE;
        let used = {
            let header = page.as_struct::<WALBucket>(0).context("wal page header")?;
            page_capacity().saturating_sub(header.free as usize)
        };
        let offset = payload_start + used;
        let bytes = page.as_mut();
        let total = record_size(occs.len());

        // Safety check
        if offset + total > bytes.len() {
            bail!("wal page write overflow");
        }

        let record = &mut bytes[offset..offset + total];
        let trig = WALTrigram {
            trigram,
            num_entries: 1,
        };
        write_struct(&mut record[..WAL_TRIGRAM_SIZE], trig);

        let entry = WALEntry {
            ctid,
            num_positions: occs.len() as u32,
        };
        let entry_start = WAL_TRIGRAM_SIZE;
        let entry_end = entry_start + WAL_ENTRY_SIZE;
        write_struct(&mut record[entry_start..entry_end], entry);

        let mut pos_off = entry_end;
        for occ in occs {
            let raw = occ.0.to_le_bytes();
            let end = pos_off + POSITION_SIZE;
            record[pos_off..end].copy_from_slice(&raw);
            pos_off = end;
        }
        Ok(())
    }

    fn flush_all(&self) -> Result<Vec<Segment>> {
        let mut wal_buf = BlockBuffer::aquire_mut(self.rel, self.wal_block);
        let header = wal_buf
            .as_struct_mut::<WALHeader>(0)
            .context("wal header")?;

        let head = header.head_block;
        if head == INVALID_BLOCK {
            return Ok(Vec::new());
        }

        // Steal the chain
        header.head_block = INVALID_BLOCK;
        header.tail_block = INVALID_BLOCK;
        header.bytes_used = 0;

        // We will free blocks as we process them.
        // We need to keep track of the free list head inside the loop?
        // Actually, we can just push them to a local list and then batch update free_head at the end
        // OR just update it one by one (slow lock).
        // Let's hold the WAL header lock? No, decoding takes time.
        // We can temporarily detach the chain, process it, then re-acquire lock to free pages.
        drop(wal_buf);

        let mut collector = Collector::new();
        let mut freed_pages = Vec::new();
        let mut occ_buf = Vec::new();
        let mut interrupt = 0u32;
        let mut current_block = Some(head);

        while let Some(block) = current_block {
            interrupt = interrupt.wrapping_add(1);
            if (interrupt & 0x3ff) == 0 {
                pg_sys::check_for_interrupts!();
            }

            let page = BlockBuffer::acquire(self.rel, block); // Read lock enough? Yes.
            let (next, used) = {
                let header = page.as_struct::<WALBucket>(0).context("wal page header")?;
                if header.magic != WAL_BUCKET_MAGIC {
                    bail!("corrupt WAL page during flush");
                }
                let used = page_capacity().saturating_sub(header.free as usize);
                (header.next_block, used)
            };

            let mut offset = PAGE_HEADER_SIZE;
            let end = PAGE_HEADER_SIZE + used;
            let bytes = page.as_ref();

            while offset < end {
                if offset + WAL_TRIGRAM_SIZE > end {
                    break; // Should error?
                }
                let trig = read_struct::<WALTrigram>(&bytes[offset..offset + WAL_TRIGRAM_SIZE]);
                offset += WAL_TRIGRAM_SIZE;

                // Assuming num_entries is 1 from our write path, but let's loop
                for _ in 0..trig.num_entries {
                    if offset + WAL_ENTRY_SIZE > end {
                        break;
                    }
                    let entry = read_struct::<WALEntry>(&bytes[offset..offset + WAL_ENTRY_SIZE]);
                    offset += WAL_ENTRY_SIZE;

                    let positions = entry.num_positions as usize;
                    let needed = positions * POSITION_SIZE;
                    if offset + needed > end {
                        break;
                    }

                    occ_buf.clear();
                    occ_buf.reserve(positions);
                    for chunk in bytes[offset..offset + needed].chunks_exact(POSITION_SIZE) {
                        occ_buf.push(Occurance(u32::from_le_bytes(chunk.try_into().unwrap())));
                    }
                    collector.add_occurrences(trig.trigram, entry.ctid, &occ_buf);
                    offset += needed;
                }
            }
            drop(page);
            freed_pages.push(block);

            current_block = if next == INVALID_BLOCK {
                None
            } else {
                Some(next)
            };
        }

        // Recycle pages
        let mut wal_buf = BlockBuffer::aquire_mut(self.rel, self.wal_block);
        let wal = wal_buf
            .as_struct_mut::<WALHeader>(0)
            .context("wal header")?;

        for block in freed_pages {
            // We need to write the "next" pointer of this block to point to current free_head
            // then update free_head.
            // This requires locking each page.
            let mut page = BlockBuffer::aquire_mut(self.rel, block);
            {
                let header = page
                    .as_struct_mut::<WALBucket>(0)
                    .context("wal page header")?;
                header.magic = WAL_BUCKET_MAGIC;
                header.free = page_capacity() as u16;
                header.next_block = wal.free_head;
            }
            wal.free_head = block;
        }

        let trgms = collector.take_trgms();
        if trgms.is_empty() {
            return Ok(Vec::new());
        }
        encode::Encoder::encode_trgms(self.rel, &trgms)
    }

    fn allocate_page(&self, free_head: &mut u32) -> Result<u32> {
        if *free_head != INVALID_BLOCK {
            let block = *free_head;
            let mut page = BlockBuffer::aquire_mut(self.rel, block);
            let next = {
                let header = page.as_struct::<WALBucket>(0).context("wal page header")?;
                header.next_block
            };
            // Reset header
            let header = page
                .as_struct_mut::<WALBucket>(0)
                .context("wal page header")?;
            header.magic = WAL_BUCKET_MAGIC;
            header.free = page_capacity() as u16;
            header.next_block = INVALID_BLOCK;

            *free_head = next;
            return Ok(block);
        }

        let mut page = BlockBuffer::allocate(self.rel);
        let header = page
            .as_struct_mut::<WALBucket>(0)
            .context("wal page header")?;
        header.magic = WAL_BUCKET_MAGIC;
        header.free = page_capacity() as u16;
        header.next_block = INVALID_BLOCK;
        Ok(page.block_number())
    }
}
