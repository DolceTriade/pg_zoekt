use crate::regex_ffi::{PgZoektRegexArc, RegexHandle};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BranchLiteralPlan {
    pub(crate) leading_anchor: bool,
    pub(crate) literals: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArcKind {
    Begin,
    End,
    Gap,
    Literal(char),
}

#[derive(Debug)]
struct ExploreCtx<'a> {
    arcs: &'a [Vec<PgZoektRegexArc>],
    kinds: &'a [ArcKind],
    final_state: usize,
    branches: std::collections::BTreeSet<BranchLiteralPlan>,
    saw_trigramless_branch: bool,
    truncated: bool,
    max_branches: usize,
    max_depth: usize,
}

#[derive(Debug, Clone)]
struct PathState {
    visited: std::collections::BTreeSet<usize>,
    literals: Vec<String>,
    current: String,
    leading_anchor: bool,
    consumed_content: bool,
    saw_literal: bool,
}

impl PathState {
    fn new(initial: usize) -> Self {
        let mut visited = std::collections::BTreeSet::new();
        visited.insert(initial);
        Self {
            visited,
            literals: Vec::new(),
            current: String::new(),
            leading_anchor: false,
            consumed_content: false,
            saw_literal: false,
        }
    }

    fn flush_current(&mut self) {
        if !self.current.is_empty() {
            self.literals.push(std::mem::take(&mut self.current));
        }
    }
}

fn canonical_literal_char(chars: &[char], case_sensitive: bool) -> Option<char> {
    if chars.is_empty() {
        return None;
    }

    let mut canonical: Option<char> = None;
    for &ch in chars {
        let mapped = if case_sensitive {
            ch
        } else {
            let mut lowered = ch.to_lowercase();
            let first = lowered.next()?;
            if lowered.next().is_some() {
                return None;
            }
            first
        };

        match canonical {
            None => canonical = Some(mapped),
            Some(prev) if prev == mapped => {}
            Some(_) => return None,
        }
    }

    canonical
}

fn build_arc_kinds(regex: &RegexHandle, case_sensitive: bool) -> Vec<ArcKind> {
    let mut out = Vec::with_capacity(regex.num_colors());
    for color in 0..regex.num_colors() {
        if regex.color_is_begin(color) {
            out.push(ArcKind::Begin);
            continue;
        }
        if regex.color_is_end(color) {
            out.push(ArcKind::End);
            continue;
        }

        let kind = regex
            .color_characters(color)
            .and_then(|chars| canonical_literal_char(&chars, case_sensitive))
            .map(ArcKind::Literal)
            .unwrap_or(ArcKind::Gap);
        out.push(kind);
    }
    out
}

fn arc_kind_for_color(kinds: &[ArcKind], color: i32) -> Option<ArcKind> {
    if color < 0 {
        return Some(ArcKind::Gap);
    }

    kinds.get(color as usize).copied()
}

fn state_requires_begin_anchor(arcs: &[PgZoektRegexArc], kinds: &[ArcKind]) -> bool {
    let mut saw_begin = false;

    for arc in arcs {
        match arc_kind_for_color(kinds, arc.co) {
            Some(ArcKind::Begin) => saw_begin = true,
            Some(_) => return false,
            None => return false,
        }
    }

    saw_begin
}

fn record_branch(ctx: &mut ExploreCtx<'_>, path: &PathState) {
    let mut branch = path.clone();
    branch.flush_current();
    let useful: Vec<String> = branch
        .literals
        .into_iter()
        .filter(|literal| literal.chars().count() >= 3)
        .collect();
    if useful.is_empty() {
        if !branch.saw_literal {
            return;
        }
        ctx.saw_trigramless_branch = true;
        return;
    }
    ctx.branches.insert(BranchLiteralPlan {
        leading_anchor: branch.leading_anchor,
        literals: useful,
    });
    if ctx.branches.len() > ctx.max_branches {
        ctx.truncated = true;
    }
}

fn explore(ctx: &mut ExploreCtx<'_>, state: usize, depth: usize, path: &PathState) {
    if ctx.truncated || depth > ctx.max_depth {
        ctx.truncated = true;
        return;
    }

    if state == ctx.final_state {
        record_branch(ctx, path);
        return;
    }

    for arc in &ctx.arcs[state] {
        let target = arc.to as usize;
        if path.visited.contains(&target) {
            continue;
        }

        let Some(kind) = arc_kind_for_color(ctx.kinds, arc.co) else {
            ctx.truncated = true;
            return;
        };

        let mut next = path.clone();
        let state_has_gap = ctx.arcs[state].iter().any(|candidate| {
            arc_kind_for_color(ctx.kinds, candidate.co).is_some_and(|kind| kind == ArcKind::Gap)
        });
        match kind {
            ArcKind::Begin => {
                if !next.consumed_content
                    && next.current.is_empty()
                    && next.literals.is_empty()
                    && state_requires_begin_anchor(&ctx.arcs[state], ctx.kinds)
                {
                    next.leading_anchor = true;
                }
            }
            ArcKind::End => {}
            ArcKind::Gap => {
                next.flush_current();
                next.consumed_content = true;
            }
            ArcKind::Literal(ch) => {
                if state_has_gap && !next.current.is_empty() {
                    next.flush_current();
                }
                next.current.push(ch);
                next.consumed_content = true;
                next.saw_literal = true;
            }
        }
        next.visited.insert(target);
        explore(ctx, target, depth + 1, &next);
        if ctx.truncated {
            return;
        }
    }
}

pub(crate) fn branch_literal_plans(
    pattern: &str,
    case_sensitive: bool,
    collation: pgrx::pg_sys::Oid,
) -> anyhow::Result<Option<Vec<BranchLiteralPlan>>> {
    let regex = RegexHandle::compile(pattern, !case_sensitive, collation)?;
    let arcs: Vec<Vec<PgZoektRegexArc>> = (0..regex.num_states())
        .map(|state| regex.out_arcs(state))
        .collect();
    let kinds = build_arc_kinds(&regex, case_sensitive);

    let mut ctx = ExploreCtx {
        arcs: &arcs,
        kinds: &kinds,
        final_state: regex.final_state(),
        branches: std::collections::BTreeSet::new(),
        saw_trigramless_branch: false,
        truncated: false,
        max_branches: 128,
        max_depth: 128,
    };
    let initial = regex.initial_state();
    let path = PathState::new(initial);
    explore(&mut ctx, initial, 0, &path);

    if ctx.truncated || ctx.saw_trigramless_branch || ctx.branches.is_empty() {
        return Ok(None);
    }

    Ok(Some(ctx.branches.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_literal_char_case_sensitive() {
        assert_eq!(canonical_literal_char(&['f'], true), Some('f'));
        assert_eq!(canonical_literal_char(&['f', 'F'], true), None);
    }

    #[test]
    fn test_canonical_literal_char_case_insensitive_ascii_fold() {
        assert_eq!(canonical_literal_char(&['f', 'F'], false), Some('f'));
        assert_eq!(canonical_literal_char(&['f', 'g'], false), None);
    }
}
