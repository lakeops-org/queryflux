//! Hot-path query fingerprinting — pure Rust character-level tokenizer.
//!
//! Produces a u64 hash in ~200 ns with no heap allocation.
//! Used by the routing layer before query execution.
//!
//! Normalization rules (same as ProxySQL / ClickHouse):
//! - Block comments `/* */`, line comments `--` and `#` → skipped
//! - String literals `'...'` and `"..."` → `\x00` sentinel fed to hasher
//! - Digit sequences → `\x00` sentinel
//! - `NULL` keyword (case-insensitive) → `\x00` sentinel
//! - Whitespace runs → single space
//! - Everything else → lowercased verbatim

use xxhash_rust::xxh64::Xxh64;

/// True when a leading `-` before digits may start a numeric literal (unary minus),
/// based on the previous ASCII byte (same-index `i - 1` in the UTF-8 stream).
#[inline]
fn prev_byte_allows_unary_minus(prev: u8) -> bool {
    matches!(
        prev,
        b'(' | b'['
            | b'{'
            | b','
            | b';'
            | b':'
            | b'='
            | b'<'
            | b'>'
            | b'!'
            | b'+'
            | b'-'
            | b'*'
            | b'/'
            | b'%'
            | b'^'
            | b'&'
            | b'|'
            | b'~'
            | b'?'
    )
}

/// Compute a fast parameterized hash of `sql` suitable for routing.
///
/// Two queries that differ only in literal values will produce the same hash.
/// No heap allocation — the hash is computed on-the-fly as the input is scanned.
pub fn fast_hash(sql: &str) -> u64 {
    let mut hasher = Xxh64::new(0);
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut last_was_space = true; // start true to trim leading whitespace

    while i < len {
        let b = bytes[i];

        // ── Block comment /* ... */ ──────────────────────────────────────────
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len {
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // ── Line comment -- ──────────────────────────────────────────────────
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // ── Line comment # ───────────────────────────────────────────────────
        if b == b'#' {
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // ── String literal '...' ─────────────────────────────────────────────
        if b == b'\'' {
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    // handle '' escape
                    if i + 1 < len && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            hasher.update(&[0x00]);
            last_was_space = false;
            continue;
        }

        // ── Double-quoted string/identifier "..." ────────────────────────────
        if b == b'"' {
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    if i + 1 < len && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            // Double-quoted tokens are usually identifiers, not literals — feed verbatim
            // (we can't distinguish at this level; router just needs consistency)
            hasher.update(&[0x00]);
            last_was_space = false;
            continue;
        }

        // ── Digit sequence (number literal) ─────────────────────────────────
        let neg_number_start = b == b'-'
            && i + 1 < len
            && bytes[i + 1].is_ascii_digit()
            && (i == 0 || last_was_space || prev_byte_allows_unary_minus(bytes[i - 1]));
        if b.is_ascii_digit() || neg_number_start {
            if b == b'-' {
                i += 1; // consume unary minus only when neg_number_start holds
            }
            while i < len
                && (bytes[i].is_ascii_digit()
                    || bytes[i] == b'.'
                    || bytes[i] == b'e'
                    || bytes[i] == b'E')
            {
                i += 1;
            }
            hasher.update(&[0x00]);
            last_was_space = false;
            continue;
        }

        // ── NULL keyword ────────────────────────────────────────────────────
        if (b == b'N' || b == b'n')
            && i + 3 < len
            && (bytes[i + 1] == b'U' || bytes[i + 1] == b'u')
            && (bytes[i + 2] == b'L' || bytes[i + 2] == b'l')
            && (bytes[i + 3] == b'L' || bytes[i + 3] == b'l')
            && (i + 4 >= len || !bytes[i + 4].is_ascii_alphanumeric())
        {
            i += 4;
            hasher.update(&[0x00]);
            last_was_space = false;
            continue;
        }

        // ── Whitespace ───────────────────────────────────────────────────────
        if b.is_ascii_whitespace() {
            if !last_was_space {
                hasher.update(b" ");
                last_was_space = true;
            }
            i += 1;
            continue;
        }

        // ── Regular character — lowercase and feed ───────────────────────────
        hasher.update(&[b.to_ascii_lowercase()]);
        last_was_space = false;
        i += 1;
    }

    hasher.digest()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_query_different_literals() {
        let a = fast_hash("SELECT * FROM t WHERE id = 1");
        let b = fast_hash("SELECT * FROM t WHERE id = 42");
        assert_eq!(a, b);
    }

    #[test]
    fn same_query_different_strings() {
        let a = fast_hash("SELECT * FROM t WHERE name = 'alice'");
        let b = fast_hash("SELECT * FROM t WHERE name = 'bob'");
        assert_eq!(a, b);
    }

    #[test]
    fn different_queries_different_hashes() {
        let a = fast_hash("SELECT * FROM users WHERE id = 1");
        let b = fast_hash("SELECT * FROM orders WHERE id = 1");
        assert_ne!(a, b);
    }

    #[test]
    fn comments_stripped() {
        let a = fast_hash("SELECT /* comment */ 1");
        let b = fast_hash("SELECT 1");
        assert_eq!(a, b);
    }

    #[test]
    fn whitespace_collapsed() {
        let a = fast_hash("SELECT   *   FROM   t");
        let b = fast_hash("SELECT * FROM t");
        assert_eq!(a, b);
    }

    #[test]
    fn null_normalized() {
        let a = fast_hash("SELECT * FROM t WHERE x = NULL");
        let b = fast_hash("SELECT * FROM t WHERE x = null");
        assert_eq!(a, b);
    }

    /// `a-1` is subtraction, not a negative literal; literals 1 vs 2 should still match.
    #[test]
    fn minus_between_identifier_and_digit_is_not_unary() {
        let a = fast_hash("SELECT a-1");
        let b = fast_hash("SELECT a-2");
        assert_eq!(a, b);
    }

    #[test]
    fn unary_minus_after_operator_without_space() {
        let a = fast_hash("WHERE id=-1");
        let b = fast_hash("WHERE id=-2");
        assert_eq!(a, b);
    }

    #[test]
    fn unary_minus_after_whitespace() {
        let a = fast_hash("WHERE id = -1");
        let b = fast_hash("WHERE id = -99");
        assert_eq!(a, b);
    }
}
