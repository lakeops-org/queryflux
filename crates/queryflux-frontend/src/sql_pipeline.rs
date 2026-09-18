//! Tracks access-control rewrite vs dialect translation as separate pipeline stages.

use queryflux_core::query::ExecutingQuery;

use crate::state::QueryContext;

/// Outcome of the SQL pipeline: OPA rewrite (source dialect) then optional dialect translation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlPipelineMeta {
    pub was_rewritten: bool,
    pub rewritten_sql: Option<String>,
    pub was_translated: bool,
    pub translated_sql: Option<String>,
}

impl SqlPipelineMeta {
    pub fn compute(access_controlled: &str, engine: &str, access_control_rewrote: bool) -> Self {
        let was_rewritten = access_control_rewrote;
        let rewritten_sql = was_rewritten.then(|| access_controlled.to_string());
        let was_translated = engine != access_controlled;
        let translated_sql = was_translated.then(|| engine.to_string());
        Self {
            was_rewritten,
            rewritten_sql,
            was_translated,
            translated_sql,
        }
    }

    /// SQL fingerprinting / engine digest: final SQL when rewrite or translation changed it.
    pub fn engine_sql_for_fingerprint<'a>(&'a self, _original: &'a str) -> Option<&'a str> {
        if self.was_translated {
            self.translated_sql.as_deref()
        } else if self.was_rewritten {
            self.rewritten_sql.as_deref()
        } else {
            None
        }
    }
}

/// Reconstruct pipeline metadata from a persisted async `ExecutingQuery`.
pub fn pipeline_from_executing(executing: &ExecutingQuery) -> (String, SqlPipelineMeta) {
    if let Some(client) = &executing.client_sql {
        let meta = SqlPipelineMeta {
            was_rewritten: executing.rewritten_sql.is_some(),
            rewritten_sql: executing.rewritten_sql.clone(),
            was_translated: executing.was_dialect_translated,
            translated_sql: executing
                .was_dialect_translated
                .then(|| executing.sql.clone()),
        };
        return (client.clone(), meta);
    }

    if let Some(original) = &executing.translated_sql {
        let changed = executing.sql != *original;
        let meta = SqlPipelineMeta {
            was_rewritten: false,
            rewritten_sql: None,
            was_translated: changed,
            translated_sql: changed.then(|| executing.sql.clone()),
        };
        return (original.clone(), meta);
    }

    (
        executing.sql.clone(),
        SqlPipelineMeta {
            was_rewritten: false,
            rewritten_sql: None,
            was_translated: false,
            translated_sql: None,
        },
    )
}

pub fn apply_pipeline(ctx: &mut QueryContext, meta: &SqlPipelineMeta) {
    ctx.was_rewritten = meta.was_rewritten;
    ctx.rewritten_sql = meta.rewritten_sql.clone();
    ctx.was_translated = meta.was_translated;
    ctx.translated_sql = meta.translated_sql.clone();
}

pub fn pipeline_fields(meta: &SqlPipelineMeta) -> (bool, Option<String>, bool, Option<String>) {
    (
        meta.was_rewritten,
        meta.rewritten_sql.clone(),
        meta.was_translated,
        meta.translated_sql.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_only_does_not_set_translated() {
        let meta = SqlPipelineMeta::compute(
            "SELECT * FROM (SELECT id FROM t WHERE region = 'EU') AS t",
            "SELECT * FROM (SELECT id FROM t WHERE region = 'EU') AS t",
            true,
        );
        assert!(meta.was_rewritten);
        assert!(meta.rewritten_sql.is_some());
        assert!(!meta.was_translated);
        assert!(meta.translated_sql.is_none());
    }

    #[test]
    fn dialect_change_sets_translated_not_rewrite() {
        let meta = SqlPipelineMeta::compute("SELECT 1", "SELECT 1 FROM dual", false);
        assert!(!meta.was_rewritten);
        assert!(meta.was_translated);
    }
}
