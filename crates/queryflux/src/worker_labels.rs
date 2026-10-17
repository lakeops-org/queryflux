//! A worker's labels (#255): `workerLabels` from its config, with the labels in
//! `workerLabelsFile` on top. The file is in the Kubernetes downward API format, one
//! `key="value"` per line, so a sidecar or DaemonSet worker reports its own pod's labels
//! while every pod shares one worker config.

use std::collections::BTreeMap;

/// Labels Kubernetes adds for its own bookkeeping; they say nothing about the workload.
const SKIPPED: &[&str] = &[
    "pod-template-hash",
    "controller-revision-hash",
    "pod-template-generation",
    "statefulset.kubernetes.io/pod-name",
    "apps.kubernetes.io/pod-index",
    "batch.kubernetes.io/controller-uid",
    "batch.kubernetes.io/job-name",
    "controller-uid",
    "job-name",
];

/// The labels to report now. A missing or unreadable file contributes nothing: the
/// downward API volume may not be mounted yet, and labels are informational.
pub(crate) fn current(
    config_labels: &BTreeMap<String, String>,
    file: Option<&str>,
) -> BTreeMap<String, String> {
    let mut labels = config_labels.clone();
    if let Some(path) = file {
        match std::fs::read_to_string(path) {
            Ok(text) => labels.extend(parse(&text)),
            Err(e) => tracing::debug!(path, "workerLabelsFile not readable: {e}"),
        }
    }
    labels
}

/// Parse downward API `key="value"` lines. Values are Go-quoted strings; a line that does
/// not parse is skipped.
pub(crate) fn parse(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| {
            let (key, value) = line.trim().split_once('=')?;
            let key = key.trim();
            if key.is_empty() || SKIPPED.contains(&key) {
                return None;
            }
            Some((key.to_string(), unquote(value.trim())?))
        })
        .collect()
}

/// Undo Go's `strconv.Quote` for the escapes Kubernetes label values can contain.
fn unquote(v: &str) -> Option<String> {
    let inner = v.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            'n' => out.push('\n'),
            't' => out.push('\t'),
            other => out.push(other),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_downward_api_labels_and_skips_bookkeeping() {
        let text = "app=\"checkout\"\nteam=\"payments\"\npod-template-hash=\"7f9c6d\"\n\
                    quoted=\"a \\\"b\\\" c\"\nbroken line\n";
        let labels = parse(text);
        assert_eq!(labels.get("app").map(String::as_str), Some("checkout"));
        assert_eq!(labels.get("team").map(String::as_str), Some("payments"));
        assert_eq!(labels.get("quoted").map(String::as_str), Some("a \"b\" c"));
        assert!(!labels.contains_key("pod-template-hash"));
        assert_eq!(labels.len(), 3);
    }

    #[test]
    fn file_labels_win_over_config_labels_and_a_missing_file_is_ignored() {
        let dir = std::env::temp_dir().join(format!("qf-labels-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("labels");
        std::fs::write(&path, "team=\"payments\"\n").unwrap();
        let config: BTreeMap<String, String> = [
            ("team".into(), "default".into()),
            ("region".into(), "eu".into()),
        ]
        .into();

        let merged = current(&config, path.to_str());
        assert_eq!(merged.get("team").map(String::as_str), Some("payments"));
        assert_eq!(merged.get("region").map(String::as_str), Some("eu"));

        assert_eq!(current(&config, Some("/nonexistent/labels")), config);
        std::fs::remove_dir_all(dir).ok();
    }
}
