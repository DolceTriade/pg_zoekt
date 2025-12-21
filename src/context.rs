use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use regex::RegexBuilder;

#[pg_extern]
pub fn pg_zoekt_extract_context_with_highlight(
    p_text: &str,
    p_substring: &str,
    p_context_lines: i32,
    p_case_sensitive: bool,
) -> TableIterator<
    'static,
    (
        name!(match_line_number, i32),
        name!(context_snippet, String),
    ),
> {
    let pattern = format!("({})", p_substring);
    let regex = RegexBuilder::new(&pattern)
        .case_insensitive(!p_case_sensitive)
        .build()
        .unwrap_or_else(|e| error!("invalid regex: {e}"));

    let context = p_context_lines.max(0) as usize;
    let bytes = p_text.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (idx, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            lines.push((start, idx));
            start = idx + 1;
        }
    }
    lines.push((start, bytes.len()));

    let mut matches = Vec::new();
    for (idx, (line_start, line_end)) in lines.iter().enumerate() {
        let line = &p_text[*line_start..*line_end];
        if regex.is_match(line) {
            matches.push(idx);
        }
    }

    if matches.is_empty() {
        return TableIterator::new(std::iter::empty());
    }

    let mut results = Vec::with_capacity(matches.len());
    let total_lines = lines.len();
    for &match_idx in &matches {
        let start_idx = match_idx.saturating_sub(context);
        let end_idx = (match_idx + context).min(total_lines.saturating_sub(1));

        let mut capacity = 0usize;
        for line_idx in start_idx..=end_idx {
            let (line_start, line_end) = lines[line_idx];
            capacity = capacity.saturating_add(line_end - line_start);
        }
        capacity = capacity.saturating_add(end_idx.saturating_sub(start_idx));
        let mut snippet = String::with_capacity(capacity);

        for line_idx in start_idx..=end_idx {
            if !snippet.is_empty() {
                snippet.push('\n');
            }
            let (line_start, line_end) = lines[line_idx];
            let line = &p_text[line_start..line_end];
            if line_idx == match_idx {
                let highlighted = regex.replace_all(line, "<mark>$1</mark>");
                snippet.push_str(&highlighted);
            } else {
                snippet.push_str(line);
            }
        }

        results.push(((match_idx + 1) as i32, snippet));
    }

    TableIterator::new(results.into_iter())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extract_context_highlight_basic() -> spi::Result<()> {
        let text = "alpha\nneedle here\nbeta\nneedle again\nomega";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let rows = client.select(
                "SELECT match_line_number, context_snippet \
                 FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4) \
                 ORDER BY match_line_number",
                None,
                &[text.into(), "needle".into(), 1.into(), true.into()],
            )?;
            let results: Vec<(i32, String)> = rows
                .into_iter()
                .map(|row| {
                    Ok((
                        row.get::<i32>(1)?.expect("line number"),
                        row.get::<String>(2)?.expect("snippet"),
                    ))
                })
                .collect::<spi::Result<_>>()?;

            assert_eq!(results.len(), 2);
            assert_eq!(
                results[0],
                (
                    2,
                    "alpha\n<mark>needle</mark> here\nbeta".to_string()
                )
            );
            assert_eq!(
                results[1],
                (
                    4,
                    "beta\n<mark>needle</mark> again\nomega".to_string()
                )
            );
            Ok(())
        })
    }

    #[pg_test]
    fn test_extract_context_highlight_case_insensitive() -> spi::Result<()> {
        let text = "First line\nNeedle in caps\nLast line";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let snippet = client
                .select(
                    "SELECT context_snippet \
                     FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4)",
                    None,
                    &[text.into(), "needle".into(), 0.into(), false.into()],
                )?
                .first()
                .get::<String>(1)?
                .unwrap_or_default();
            assert_eq!(snippet, "<mark>Needle</mark> in caps");
            Ok(())
        })
    }

    #[pg_test]
    fn test_extract_context_highlight_negative_context() -> spi::Result<()> {
        let text = "line one\nmatch me\nline three";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let snippet = client
                .select(
                    "SELECT context_snippet \
                     FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4)",
                    None,
                    &[text.into(), "match".into(), (-5).into(), true.into()],
                )?
                .first()
                .get::<String>(1)?
                .unwrap_or_default();
            assert_eq!(snippet, "<mark>match</mark> me");
            Ok(())
        })
    }

    #[pg_test]
    fn test_extract_context_highlight_no_matches() -> spi::Result<()> {
        let text = "alpha\nbeta\ngamma";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let count = client
                .select(
                    "SELECT count(*) \
                     FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4)",
                    None,
                    &[text.into(), "needle".into(), 2.into(), true.into()],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or(0);
            assert_eq!(count, 0);
            Ok(())
        })
    }
}
