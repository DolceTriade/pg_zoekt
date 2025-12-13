/// Trigram stuff
use std::{collections::BTreeMap, str::CharIndices};

use anyhow::Context;

pub struct CompactTrgm(pub u32);

impl CompactTrgm {
    pub fn is_lossy(&self) -> bool {
        self.0 & 1_u32 << 31 != 0
    }

    pub fn case_bits(&self) -> u8 {
        (self.0 >> 24 & 0b111) as u8
    }

    pub fn flags(&self) -> u8 {
        (self.0 >> 24) as u8
    }

    pub fn trgm(&self) -> u32 {
        self.0 & 0xffffff
    }

    pub fn txt(&self) -> String {
        let t = [
            (self.0 & 0xff) as u8,
            (self.0 >> 8 & 0xff) as u8,
            (self.0 >> 16 & 0xff) as u8,
        ];
        String::from_utf8_lossy(&t).to_string()
    }
}

impl std::fmt::Debug for CompactTrgm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CompactTrgm")
            .field(&self.txt())
            .field(&self.flags())
            .finish()
    }
}

impl TryFrom<&str> for CompactTrgm {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bits = value.chars().enumerate().try_fold(
            (0_u8, 0_u32, false),
            |(flags, trgm, lossy), (idx, chr): (usize, char)| -> anyhow::Result<(u8, u32, bool)> {
                if idx >= 3 {
                    anyhow::bail!("not a trigram: {value}");
                }
                Ok((
                    chr.is_uppercase()
                        .then(|| flags | 1 << idx)
                        .unwrap_or(flags),
                    chr.is_ascii()
                        .then(|| trgm | (chr.to_ascii_lowercase() as u32) << (idx * 8))
                        .unwrap_or_else(|| {
                            let hashed =
                                crc32fast::hash(chr.to_lowercase().to_string().as_bytes()) & 0xff;
                            trgm | hashed << (idx * 8)
                        }),
                    !chr.is_ascii() || lossy,
                ))
            },
        )?;
        if bits.1 & 0xff == 0 || bits.1 >> 8 & 0xff == 0 || bits.1 >> 16 & 0xff == 0 {
            anyhow::bail!("not a trigram: {value} {}", bits.1);
        }
        let mut combined = bits.1 | (bits.0 as u32) << 24;
        if bits.2 {
            combined |= 1_u32 << 31;
        }
        Ok(CompactTrgm(combined))
    }
}

pub struct Extractor<'a> {
    txt: &'a str,
    chars: CharIndices<'a>,
    window: [usize; 3],
    have_window: bool,
    next_char_start: Option<usize>,
    len: usize,
}

impl<'a> Extractor<'a> {
    pub fn extract(txt: &'a str) -> Self {
        let mut chars = txt.char_indices();
        let mut window = [0usize; 3];
        let mut filled = 0;

        // Prime the first 3 character starts.
        while filled < 3 {
            match chars.next() {
                Some((off, _)) => {
                    window[filled] = off;
                    filled += 1;
                }
                None => break,
            }
        }

        let have_window = filled == 3;
        let next_char_start = if have_window {
            chars.next().map(|(off, _)| off)
        } else {
            None
        };

        Self {
            txt,
            chars,
            window,
            have_window,
            next_char_start,
            len: txt.len(),
        }
    }
}

impl<'a> Iterator for Extractor<'a> {
    type Item = (&'a str, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.have_window {
            return None;
        }

        let start = self.window[0];
        let end = self.next_char_start.unwrap_or(self.len);

        // Advance the window by one character.
        if let Some(next_start) = self.next_char_start {
            self.window = [self.window[1], self.window[2], next_start];
            self.next_char_start = self.chars.next().map(|(off, _)| off);
            if self.next_char_start.is_none() {
                // Next iteration will use len as end boundary, then stop.
                self.next_char_start = None;
            }
        } else {
            self.have_window = false;
        }

        Some((&self.txt[start..end], start))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Occurance(pub u32);

impl Occurance {
    pub fn position(&self) -> u32 {
        self.0 & 0xffffff
    }

    pub fn is_lossy(&self) -> bool {
        self.0 & 1_u32 << 31 != 0
    }

    pub fn case_bits(&self) -> u8 {
        (self.0 >> 24 & 0b111) as u8
    }

    pub fn flags(&self) -> u8 {
        (self.0 >> 24) as u8
    }

    pub fn set_flags(&mut self, flags: u8) {
        self.0 &= 0xffffff;
        self.0 |= (flags as u32) << 24;
    }
}

#[derive(Debug)]
pub struct Collector {
    trgms: BTreeMap<u32, BTreeMap<crate::storage::ItemPointer, Vec<Occurance>>>,
    size_estimate: usize,
}

impl Collector {
    pub fn new() -> Self {
        Self {
            trgms: BTreeMap::new(),
            size_estimate: 0,
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.size_estimate
    }

    pub fn take_trgms(
        &mut self,
    ) -> BTreeMap<u32, BTreeMap<crate::storage::ItemPointer, Vec<Occurance>>> {
        self.size_estimate = 0;
        std::mem::take(&mut self.trgms)
    }

    pub fn add(
        &mut self,
        ctid: crate::storage::ItemPointer,
        trgm: &str,
        position: u32,
    ) -> anyhow::Result<()> {
        let ct = CompactTrgm::try_from(trgm).context("failed to parse trgm")?;
        if position >> 24 > 0 {
            anyhow::bail!("Position too large to be indexed: {position}");
        }
        let mut o = Occurance(position);
        o.set_flags(ct.flags());
        self.trgms
            .entry(ct.trgm())
            .or_insert(BTreeMap::new())
            .entry(ctid)
            .or_insert(Vec::new())
            .push(o);
        self.size_estimate +=
            std::mem::size_of_val(&ct) + std::mem::size_of_val(&o) + std::mem::size_of_val(&ctid);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_extract() {
        let s = "abcdefgh";
        let trgms = Extractor::extract(s).map(|t| t.0).collect::<Vec<&str>>();
        assert_eq!(trgms, vec!["abc", "bcd", "cde", "def", "efg", "fgh"]);

        let s = "aβγδ";
        let trgms = Extractor::extract(s).map(|t| t.0).collect::<Vec<&str>>();
        assert_eq!(trgms, vec!["aβγ", "βγδ"]);

        let s = "";
        let trgms = Extractor::extract(s).map(|t| t.0).collect::<Vec<&str>>();
        assert_eq!(trgms, Vec::<&str>::new());

        let s = "a";
        let trgms = Extractor::extract(s).map(|t| t.0).collect::<Vec<&str>>();
        assert_eq!(trgms, Vec::<&str>::new());
    }

    #[test]
    pub fn test_compact() {
        _ = CompactTrgm::try_from("abcd").expect_err("Expected error");
        let a = CompactTrgm::try_from("abc").expect("not error");
        assert_eq!(a.0, 0x636261);
        assert!(!a.is_lossy());
        assert_eq!(a.case_bits(), 0);
        assert_eq!(a.trgm(), 0x636261);
        let a = CompactTrgm::try_from("Abc").expect("not error");
        assert_eq!(a.0, 0x1636261);
        assert!(!a.is_lossy());
        assert_eq!(a.case_bits(), 0b001);
        let a = CompactTrgm::try_from("aBc").expect("not error");
        assert_eq!(a.0, 0x2636261);
        assert_eq!(a.trgm(), 0x636261);
        assert!(!a.is_lossy());
        assert_eq!(a.case_bits(), 0b010);
        let a = CompactTrgm::try_from("abC").expect("not error");
        assert_eq!(a.0, 0x4636261);
        assert!(!a.is_lossy());
        assert_eq!(a.case_bits(), 0b100);
        let a = CompactTrgm::try_from("ABC").expect("not error");
        assert_eq!(a.0, 0x7636261);
        assert!(!a.is_lossy());
        assert_eq!(a.case_bits(), 0b111);
        let a = CompactTrgm::try_from("aβΒ").expect("not error");
        assert!(a.is_lossy());
        assert_eq!(a.case_bits(), 0b100);
        assert_eq!(a.0, 0x849f9f61);
    }
}
