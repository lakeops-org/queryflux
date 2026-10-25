//! Fallback fingerprinting — regex-based, used when polyglot-sql parse fails.

use xxhash_rust::xxh64::xxh64;

/// Normalize `sql` without an AST parser.
///
/// Returns `(query_hash, parameterized_hash, digest_text)`.
/// Used as fallback when polyglot-sql cannot parse the input.
pub fn fallback_fingerprint(sql: &str) -> (u64, u64, String) {
    let normalized = normalize_raw(sql);
    let query_hash = xxh64(normalized.as_bytes(), 0);
    let digest = parameterize(&normalized);
    let parameterized_hash = xxh64(digest.as_bytes(), 0);
    (query_hash, parameterized_hash, digest)
}

/// Strip comments, collapse whitespace, lowercase — no literal replacement.
pub fn normalize_raw(sql: &str) -> String {
    // Strip block comments
    let s = strip_block_comments(sql);
    // Strip line comments
    let s = strip_line_comments(&s);
    // Collapse whitespace
    let s = collapse_whitespace(&s);
    s.to_lowercase()
}

/// Apply literal replacement on already-normalized SQL.
pub fn parameterize(normalized: &str) -> String {
    // Replace string literals first (before numbers, to avoid matching inside strings)
    let s = replace_string_literals(normalized);
    // Replace number literals
    let s = replace_number_literals(&s);
    // Replace NULL
    let s = replace_null(&s);
    // Collapse IN lists: IN (?, ?, ...) → IN (?.. )
    let s = collapse_in_lists(&s);
    // Collapse VALUES lists
    collapse_values_lists(&s)
}

fn strip_block_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() {
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn strip_line_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    for line in sql.lines() {
        // Find -- or # that's not inside a string (simplified: just find first occurrence)
        let stripped = strip_line_comment(line);
        out.push_str(stripped);
        out.push(' ');
    }
    out
}

fn strip_line_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_string = false;
    let mut string_char = b'\'';
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == string_char {
                in_string = false;
            }
        } else if b == b'\'' || b == b'"' {
            in_string = true;
            string_char = b;
        } else if (b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-') || b == b'#' {
            return &line[..i];
        }
        i += 1;
    }
    line
}

fn collapse_whitespace(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut last_space = true;
    for c in sql.chars() {
        if c.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(c);
            last_space = false;
        }
    }
    out.trim().to_string()
}

fn replace_string_literals(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Consume string literal
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push('?');
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn replace_number_literals(sql: &str) -> String {
    // Match standalone digit sequences (not part of identifiers)
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // Check if this is the start of a number (not preceded by an identifier char)
        let prev_is_ident = i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
        if c.is_ascii_digit() && !prev_is_ident {
            // Consume digits, optional decimal, optional exponent
            while i < chars.len()
                && (chars[i].is_ascii_digit()
                    || chars[i] == '.'
                    || chars[i] == 'e'
                    || chars[i] == 'E')
            {
                i += 1;
            }
            out.push('?');
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

fn replace_null(sql: &str) -> String {
    // Case-insensitive NULL replacement at word boundaries (char-safe for UTF-8).
    let seq: Vec<(usize, char)> = sql.char_indices().collect();
    let mut out = String::with_capacity(sql.len());
    let mut pos = 0usize;
    while pos < seq.len() {
        if pos + 3 < seq.len() {
            let (_, a) = seq[pos];
            let (_, b) = seq[pos + 1];
            let (_, c) = seq[pos + 2];
            let (_, d) = seq[pos + 3];
            if a.eq_ignore_ascii_case(&'n')
                && b.eq_ignore_ascii_case(&'u')
                && c.eq_ignore_ascii_case(&'l')
                && d.eq_ignore_ascii_case(&'l')
            {
                let prev_ok = pos == 0 || !seq[pos - 1].1.is_ascii_alphanumeric();
                let next_ok = pos + 4 >= seq.len() || !seq[pos + 4].1.is_ascii_alphanumeric();
                if prev_ok && next_ok {
                    out.push('?');
                    pos += 4;
                    continue;
                }
            }
        }
        out.push(seq[pos].1);
        pos += 1;
    }
    out
}

fn collapse_in_lists(sql: &str) -> String {
    // Replace IN (?, ?, ...) with IN (?.. ) when there are 2+ items
    // Case-insensitive match on IN
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re =
        RE.get_or_init(|| regex::Regex::new(r"(?i)\bIN\s*\(\s*\?(?:\s*,\s*\?)+\s*\)").unwrap());
    re.replace_all(sql, "IN (?.. )").into_owned()
}

fn collapse_values_lists(sql: &str) -> String {
    // Replace VALUES (...), (...), ... with VALUES (?.. ) when multiple rows
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)\bVALUES\s*\(\s*\?(?:\s*,\s*\?)*\s*\)(?:\s*,\s*\(\s*\?(?:\s*,\s*\?)*\s*\))+",
        )
        .unwrap()
    });
    re.replace_all(sql, "VALUES (?.. )").into_owned()
}
