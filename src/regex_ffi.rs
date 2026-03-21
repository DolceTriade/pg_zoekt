use pgrx::prelude::pg_sys;

#[repr(C)]
pub(crate) struct PgZoektRegexHandle {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PgZoektRegexArc {
    pub(crate) co: i32,
    pub(crate) to: i32,
}

unsafe extern "C" {
    fn pg_zoekt_regex_compile(
        pattern: *const std::ffi::c_char,
        pattern_len: i32,
        case_insensitive: bool,
        collation: pg_sys::Oid,
        errcode: *mut i32,
    ) -> *mut PgZoektRegexHandle;
    fn pg_zoekt_regex_free(handle: *mut PgZoektRegexHandle);
    fn pg_zoekt_regex_num_states(handle: *const PgZoektRegexHandle) -> i32;
    fn pg_zoekt_regex_initial_state(handle: *const PgZoektRegexHandle) -> i32;
    fn pg_zoekt_regex_final_state(handle: *const PgZoektRegexHandle) -> i32;
    fn pg_zoekt_regex_num_out_arcs(handle: *const PgZoektRegexHandle, state: i32) -> i32;
    fn pg_zoekt_regex_get_out_arcs(
        handle: *const PgZoektRegexHandle,
        state: i32,
        arcs: *mut PgZoektRegexArc,
        arcs_len: i32,
    );
    fn pg_zoekt_regex_num_colors(handle: *const PgZoektRegexHandle) -> i32;
    fn pg_zoekt_regex_color_is_begin(handle: *const PgZoektRegexHandle, color: i32) -> bool;
    fn pg_zoekt_regex_color_is_end(handle: *const PgZoektRegexHandle, color: i32) -> bool;
    fn pg_zoekt_regex_num_characters(handle: *const PgZoektRegexHandle, color: i32) -> i32;
    fn pg_zoekt_regex_get_characters(
        handle: *const PgZoektRegexHandle,
        color: i32,
        chars: *mut pg_sys::pg_wchar,
        chars_len: i32,
    );
}

pub(crate) struct RegexHandle(*mut PgZoektRegexHandle);

impl RegexHandle {
    pub(crate) fn compile(
        pattern: &str,
        case_insensitive: bool,
        collation: pg_sys::Oid,
    ) -> anyhow::Result<Self> {
        let mut errcode = 0_i32;
        let handle = unsafe {
            pg_zoekt_regex_compile(
                pattern.as_ptr() as *const std::ffi::c_char,
                i32::try_from(pattern.len()).unwrap_or(i32::MAX),
                case_insensitive,
                collation,
                &mut errcode,
            )
        };
        if handle.is_null() {
            anyhow::bail!("regex compile failed with code {errcode}");
        }
        Ok(Self(handle))
    }

    pub(crate) fn num_states(&self) -> usize {
        unsafe { pg_zoekt_regex_num_states(self.0) as usize }
    }

    pub(crate) fn initial_state(&self) -> usize {
        unsafe { pg_zoekt_regex_initial_state(self.0) as usize }
    }

    pub(crate) fn final_state(&self) -> usize {
        unsafe { pg_zoekt_regex_final_state(self.0) as usize }
    }

    pub(crate) fn out_arcs(&self, state: usize) -> Vec<PgZoektRegexArc> {
        let len = unsafe { pg_zoekt_regex_num_out_arcs(self.0, state as i32) };
        if len <= 0 {
            return Vec::new();
        }
        let mut arcs = vec![PgZoektRegexArc { co: 0, to: 0 }; len as usize];
        unsafe {
            pg_zoekt_regex_get_out_arcs(self.0, state as i32, arcs.as_mut_ptr(), len);
        }
        arcs
    }

    pub(crate) fn num_colors(&self) -> usize {
        unsafe { pg_zoekt_regex_num_colors(self.0) as usize }
    }

    pub(crate) fn color_is_begin(&self, color: usize) -> bool {
        unsafe { pg_zoekt_regex_color_is_begin(self.0, color as i32) }
    }

    pub(crate) fn color_is_end(&self, color: usize) -> bool {
        unsafe { pg_zoekt_regex_color_is_end(self.0, color as i32) }
    }

    pub(crate) fn color_characters(&self, color: usize) -> Option<Vec<char>> {
        let len = unsafe { pg_zoekt_regex_num_characters(self.0, color as i32) };
        if len <= 0 || len > 32 {
            return None;
        }
        let mut chars = vec![0 as pg_sys::pg_wchar; len as usize];
        unsafe {
            pg_zoekt_regex_get_characters(self.0, color as i32, chars.as_mut_ptr(), len);
        }
        let mut out = Vec::with_capacity(chars.len());
        for chr in chars {
            let Some(ch) = char::from_u32(chr) else {
                return None;
            };
            out.push(ch);
        }
        Some(out)
    }
}

impl Drop for RegexHandle {
    fn drop(&mut self) {
        unsafe {
            pg_zoekt_regex_free(self.0);
        }
    }
}
