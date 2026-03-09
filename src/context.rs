use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use pgrx::JsonB;
use regex::RegexBuilder;
use serde_json::json;

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
        name!(snippet_start_line_number, i32),
        name!(context_snippet, String),
        name!(match_spans, JsonB),
    ),
> {
    let regex = RegexBuilder::new(p_substring)
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
            snippet.push_str(&p_text[line_start..line_end]);
        }

        let snippet_start_line_number = (start_idx + 1) as i32;
        let match_line_number = (match_idx + 1) as i32;
        let match_line = &p_text[lines[match_idx].0..lines[match_idx].1];
        let line_offset_bytes = if match_idx == start_idx {
            0usize
        } else {
            let previous_start = lines[start_idx].0;
            let previous_end = lines[match_idx - 1].1;
            previous_end - previous_start + 1
        };

        let mut spans = Vec::new();
        let mut search_start = 0usize;
        while search_start <= match_line.len() {
            let Some(found) = regex.find_at(match_line, search_start) else {
                break;
            };

            spans.push(json!({
                "start": line_offset_bytes + found.start(),
                "end": line_offset_bytes + found.end(),
            }));

            if found.end() <= found.start() {
                if let Some(next_ch) = match_line[found.start()..].chars().next() {
                    search_start = found.start() + next_ch.len_utf8();
                } else {
                    break;
                }
            } else {
                search_start = found.end();
            }
        }

        results.push((
            match_line_number,
            snippet_start_line_number,
            snippet,
            JsonB(serde_json::Value::Array(spans)),
        ));
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
                "SELECT match_line_number, snippet_start_line_number, context_snippet, match_spans::text \
                 FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4) \
                 ORDER BY match_line_number",
                None,
                &[text.into(), "needle".into(), 1.into(), true.into()],
            )?;
            let results: Vec<(i32, i32, String, String)> = rows
                .into_iter()
                .map(|row| {
                    Ok((
                        row.get::<i32>(1)?.expect("line number"),
                        row.get::<i32>(2)?.expect("snippet start line number"),
                        row.get::<String>(3)?.expect("snippet"),
                        row.get::<String>(4)?.expect("match spans"),
                    ))
                })
                .collect::<spi::Result<_>>()?;

            assert_eq!(results.len(), 2);
            assert_eq!(
                results[0],
                (
                    2,
                    1,
                    "alpha\nneedle here\nbeta".to_string(),
                    "[{\"end\": 12, \"start\": 6}]".to_string(),
                )
            );
            assert_eq!(
                results[1],
                (
                    4,
                    3,
                    "beta\nneedle again\nomega".to_string(),
                    "[{\"end\": 11, \"start\": 5}]".to_string(),
                )
            );
            Ok(())
        })
    }

    #[pg_test]
    fn test_extract_context_highlight_case_insensitive() -> spi::Result<()> {
        let text = "First line\nNeedle in caps\nLast line";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let row = client
                .select(
                    "SELECT match_line_number, snippet_start_line_number, context_snippet, match_spans::text \
                     FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4)",
                    None,
                    &[text.into(), "needle".into(), 0.into(), false.into()],
                )?
                .first();
            assert_eq!(row.get::<i32>(1)?.unwrap_or_default(), 2);
            assert_eq!(row.get::<i32>(2)?.unwrap_or_default(), 2);
            assert_eq!(
                row.get::<String>(3)?.unwrap_or_default(),
                "Needle in caps".to_string()
            );
            assert_eq!(
                row.get::<String>(4)?.unwrap_or_default(),
                "[{\"end\": 6, \"start\": 0}]".to_string()
            );
            Ok(())
        })
    }

    #[pg_test]
    fn test_extract_context_highlight_negative_context() -> spi::Result<()> {
        let text = "line one\nmatch me\nline three";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let row = client
                .select(
                    "SELECT match_line_number, snippet_start_line_number, context_snippet, match_spans::text \
                     FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4)",
                    None,
                    &[text.into(), "match".into(), (-5).into(), true.into()],
                )?
                .first();
            assert_eq!(row.get::<i32>(1)?.unwrap_or_default(), 2);
            assert_eq!(row.get::<i32>(2)?.unwrap_or_default(), 2);
            assert_eq!(row.get::<String>(3)?.unwrap_or_default(), "match me");
            assert_eq!(
                row.get::<String>(4)?.unwrap_or_default(),
                "[{\"end\": 5, \"start\": 0}]".to_string()
            );
            Ok(())
        })
    }

    #[pg_test]
    fn test_extract_context_highlight_multiple_spans_same_line() -> spi::Result<()> {
        let text = "zero\nneedle and needle\nlast";
        Spi::connect_mut(|client| -> spi::Result<()> {
            let row = client
                .select(
                    "SELECT match_line_number, snippet_start_line_number, context_snippet, match_spans::text \
                     FROM pg_zoekt_extract_context_with_highlight($1, $2, $3, $4)",
                    None,
                    &[text.into(), "needle".into(), 1.into(), true.into()],
                )?
                .first();
            assert_eq!(row.get::<i32>(1)?.unwrap_or_default(), 2);
            assert_eq!(row.get::<i32>(2)?.unwrap_or_default(), 1);
            assert_eq!(
                row.get::<String>(3)?.unwrap_or_default(),
                "zero\nneedle and needle\nlast".to_string()
            );
            assert_eq!(
                row.get::<String>(4)?.unwrap_or_default(),
                "[{\"end\": 11, \"start\": 5}, {\"end\": 22, \"start\": 16}]".to_string()
            );
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
