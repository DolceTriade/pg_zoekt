/// Trigram stuff
use std::str::CharIndices;

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
    type Item = &'a str;

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

        Some(&self.txt[start..end])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_extract() {
        let s = "abcdefgh";
        let trgms = Extractor::extract(s).collect::<Vec<&str>>();
        assert_eq!(trgms, vec!["abc", "bcd", "cde", "def", "efg", "fgh"]);

        let s = "aβγδ";
        let trgms = Extractor::extract(s).collect::<Vec<&str>>();
        assert_eq!(trgms, vec!["aβγ", "βγδ"]);

        let s = "";
        let trgms = Extractor::extract(s).collect::<Vec<&str>>();
        assert_eq!(trgms, Vec::<&str>::new());

        let s = "a";
        let trgms = Extractor::extract(s).collect::<Vec<&str>>();
        assert_eq!(trgms, Vec::<&str>::new());
    }
}
