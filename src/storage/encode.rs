use super::pgbuffer::BlockBuffer;
use anyhow::Context;
use pgrx::prelude::*;
use zerocopy::{TryFromBytes, pointer};

pub struct Encoder<'a> {
    collector: &'a crate::trgm::Collector,
}

impl<'a> Encoder<'a> {
    pub fn new(collector: &'a crate::trgm::Collector) -> Self {
        Self { collector }
    }

    pub fn encode(&self, rel: pg_sys::Relation) -> anyhow::Result<super::Segment> {
        let trgms = self.collector.trgms();
        let mut meta1: Option<BlockBuffer> = None;
        let mut remaining_trgms = trgms.len();
        let mut iter = trgms.iter();
        info!("Encoding {remaining_trgms}");
        while remaining_trgms > 0 {
            let mut leaf = BlockBuffer::allocate(rel);
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
                .as_struct_with_elems_mut::<super::IndexList>(BH_SIZE, num_entries, ENTRY_SIZE)
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
        Ok(super::Segment { block: 0, size: 0 })
    }
}
