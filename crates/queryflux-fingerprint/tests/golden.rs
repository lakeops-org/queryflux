//! Fingerprints must not change: `query_hash` keys stored history, so a parser upgrade that
//! changes one silently splits a query's history in two. Recorded with polyglot-sql 0.9.2.
//!
//! `UPDATE_GOLDEN=1 cargo test --test golden` rewrites the expected values; only do that
//! when a change in fingerprints is intended and its migration is planned.

use queryflux_fingerprint::rich_fingerprint;
use serde_json::{json, Value};

const CORPUS: &str = include_str!("fixtures/golden_corpus.json");
const GOLDEN: &str = "tests/fixtures/golden_fingerprints.json";

#[test]
fn fingerprints_are_stable() {
    let corpus: Vec<Value> = serde_json::from_str(CORPUS).unwrap();
    let got: Vec<Value> = corpus
        .iter()
        .map(|c| {
            let (sql, d) = (c["sql"].as_str().unwrap(), c["dialect"].as_str().unwrap());
            match rich_fingerprint(sql, None, d, d) {
                // u64s as strings: JSON numbers above 2^53 lose precision in many readers.
                Some(f) => json!({
                    "query_hash": f.query_hash.to_string(),
                    "query_parameterized_hash": f.query_parameterized_hash.to_string(),
                    "digest_text": f.digest_text,
                }),
                None => Value::Null,
            }
        })
        .collect();
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN, serde_json::to_string_pretty(&got).unwrap()).unwrap();
        return;
    }
    let want: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(GOLDEN).expect("golden file")).unwrap();
    assert_eq!(want.len(), got.len());
    for (i, (w, g)) in want.iter().zip(&got).enumerate() {
        assert_eq!(w, g, "fingerprint changed for statement {i}: {}", corpus[i]["sql"]);
    }
}
