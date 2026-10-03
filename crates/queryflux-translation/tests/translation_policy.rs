use queryflux_core::{
    config::TranslationMode,
    query::{SqlDialect, TranslationReason, TranslationStatus},
};
use queryflux_translation::{ColumnMap, SchemaContext, TranslationService};

#[tokio::test]
async fn best_effort_preserves_original_sql_on_transpile_error() {
    let service = TranslationService::new_sqlglot(vec![]).unwrap();
    let sql = "SELECT FROM WHERE";
    let result = service
        .maybe_translate(
            sql,
            &SqlDialect::Trino,
            &SqlDialect::DuckDb,
            &SchemaContext::default(),
            &[],
        )
        .await;
    assert_eq!(result.unwrap(), sql);
}

#[tokio::test]
async fn unavailable_translation_obeys_policy_and_preserves_fixup_requirements() {
    for mode in [TranslationMode::BestEffort, TranslationMode::Strict] {
        let service = TranslationService::unavailable(vec![]).with_policy(mode, false);
        let report = service
            .maybe_translate_report(
                "SELECT 1",
                &SqlDialect::Trino,
                &SqlDialect::DuckDb,
                &SchemaContext::default(),
                &[],
            )
            .await;
        assert_eq!(report.outcome.status, TranslationStatus::No);
        assert_eq!(
            report.outcome.reason,
            Some(TranslationReason::SqlglotUnavailable)
        );
        assert_eq!(report.result.is_err(), mode == TranslationMode::Strict);
        let compatible = service
            .maybe_translate_report(
                "SELECT 1",
                &SqlDialect::MySql,
                &SqlDialect::StarRocks,
                &SchemaContext::default(),
                &[],
            )
            .await;
        assert_eq!(compatible.result.unwrap(), "SELECT 1");
        assert_eq!(
            compatible.outcome.reason,
            Some(TranslationReason::NotNeeded)
        );
    }
    // Global and group fixups both require Python, even on compatible dialects.
    for (global, group) in [
        (vec!["script".into()], vec![]),
        (vec![], vec!["script".into()]),
    ] {
        let service =
            TranslationService::unavailable(global).with_policy(TranslationMode::Strict, false);
        let report = service
            .maybe_translate_report(
                "SELECT 1",
                &SqlDialect::Trino,
                &SqlDialect::Trino,
                &SchemaContext::default(),
                &group,
            )
            .await;
        assert!(report
            .result
            .unwrap_err()
            .to_string()
            .contains("required translation unavailable"));
    }
}

#[tokio::test]
async fn strict_rejects_parse_and_unsupported_errors() {
    // The legacy flag also enables fail-closed behavior.
    for (mode, legacy) in [
        (TranslationMode::Strict, false),
        (TranslationMode::BestEffort, true),
    ] {
        let service = TranslationService::new_sqlglot(vec![])
            .unwrap()
            .with_policy(mode, legacy);
        for sql in ["SELECT FROM WHERE", "SELECT APPROX_DISTINCT(x, 0.1) FROM t"] {
            let report = service
                .maybe_translate_report(
                    sql,
                    &SqlDialect::Trino,
                    &SqlDialect::DuckDb,
                    &SchemaContext::default(),
                    &[],
                )
                .await;
            assert!(report.result.is_err(), "accepted {sql}");
            assert_eq!(
                report.outcome.reason,
                Some(TranslationReason::TranspileError)
            );
            assert_eq!(report.outcome.status, TranslationStatus::No);
        }
    }
}

#[tokio::test]
async fn schema_outcomes_are_independent_of_text_changes_and_strictness() {
    let schema = SchemaContext {
        tables: std::collections::HashMap::from([(
            "t".into(),
            ColumnMap::from([("x".into(), "INT".into())]),
        )]),
        ..Default::default()
    };
    for mode in [TranslationMode::BestEffort, TranslationMode::Strict] {
        let service = TranslationService::new_sqlglot(vec![])
            .unwrap()
            .with_policy(mode, false);
        let no_schema = service
            .maybe_translate_report(
                "SELECT 1",
                &SqlDialect::Trino,
                &SqlDialect::DuckDb,
                &SchemaContext::default(),
                &[],
            )
            .await;
        assert_eq!(no_schema.result.unwrap(), "SELECT 1");
        assert_eq!(no_schema.outcome.status, TranslationStatus::Fallback);
        assert_eq!(no_schema.outcome.reason, Some(TranslationReason::NoSchema));
        let optimized = service
            .maybe_translate_report(
                "SELECT x FROM t",
                &SqlDialect::Trino,
                &SqlDialect::DuckDb,
                &schema,
                &[],
            )
            .await;
        assert!(optimized.result.unwrap().contains("\"t\".\"x\""));
        assert_eq!(optimized.outcome.status, TranslationStatus::Yes);
        assert_eq!(optimized.outcome.reason, None);
        let fallback = service
            .maybe_translate_report(
                "SELECT missing FROM t",
                &SqlDialect::Trino,
                &SqlDialect::DuckDb,
                &schema,
                &[],
            )
            .await;
        assert_eq!(fallback.result.unwrap(), "SELECT missing FROM t");
        assert_eq!(fallback.outcome.status, TranslationStatus::Fallback);
        assert_eq!(
            fallback.outcome.reason,
            Some(TranslationReason::OptimizeError)
        );
    }
}

#[tokio::test]
async fn fixup_failure_obeys_policy() {
    for mode in [TranslationMode::BestEffort, TranslationMode::Strict] {
        let service = TranslationService::new_sqlglot(vec![
            "def transform(sql, src, dst):\n    if 'orders' in sql:\n        raise ValueError('broken fixup')\n    return sql".into(),
        ])
        .unwrap()
        .with_policy(mode, false);
        let report = service
            .maybe_translate_report(
                "select id from orders",
                &SqlDialect::Trino,
                &SqlDialect::Trino,
                &SchemaContext::default(),
                &[],
            )
            .await;
        assert_eq!(
            report.outcome.reason,
            Some(TranslationReason::TranspileError)
        );
        if mode == TranslationMode::Strict {
            assert!(report.result.is_err());
        } else {
            assert_eq!(report.result.unwrap(), "select id from orders");
        }
    }
}

/// Statement-kind rejections remain fatal under every policy and fixup scope.
#[tokio::test]
async fn read_to_write_fixup_rejection_cannot_fall_back() {
    let script = "def transform(sql, src, dst):\n    if 'orders' in sql:\n        return 'DELETE FROM orders'\n    return sql";
    for (mode, legacy) in [
        (TranslationMode::BestEffort, false),
        (TranslationMode::Strict, false),
        (TranslationMode::BestEffort, true),
    ] {
        for global in [false, true] {
            let scripts = vec![script.to_string()];
            let (global_scripts, group_scripts) = if global {
                (scripts, vec![])
            } else {
                (vec![], scripts)
            };
            let service = TranslationService::new_sqlglot(global_scripts)
                .unwrap()
                .with_policy(mode, legacy);
            for target in [SqlDialect::Trino, SqlDialect::DuckDb] {
                let report = service
                    .maybe_translate_report(
                        "SELECT id FROM orders",
                        &SqlDialect::Trino,
                        &target,
                        &SchemaContext::default(),
                        &group_scripts,
                    )
                    .await;
                let error = report.result.expect_err(
                    "a statement-kind rejection must not become successful passthrough",
                );
                assert!(error.to_string().contains("changed the statement kind"));
                assert_eq!(report.outcome.status, TranslationStatus::No);
                assert_eq!(
                    report.outcome.reason,
                    Some(TranslationReason::TranspileError)
                );
            }
        }
    }
}

#[tokio::test]
async fn opaque_commands_and_multiple_statements_cannot_silently_succeed() {
    for mode in [TranslationMode::BestEffort, TranslationMode::Strict] {
        let service = TranslationService::new_sqlglot(vec![])
            .unwrap()
            .with_policy(mode, false);
        for sql in ["VACUUM t", "SELECT 1; SELECT 2"] {
            let report = service
                .maybe_translate_report(
                    sql,
                    &SqlDialect::Trino,
                    &SqlDialect::DuckDb,
                    &SchemaContext::default(),
                    &[],
                )
                .await;
            assert_eq!(report.outcome.status, TranslationStatus::No, "{sql}");
            assert_eq!(
                report.outcome.reason,
                Some(TranslationReason::TranspileError)
            );
            if mode == TranslationMode::Strict {
                assert!(report.result.is_err(), "accepted {sql}");
            } else {
                assert_eq!(report.result.unwrap(), sql);
            }
        }
    }
}

#[tokio::test]
async fn leading_empty_statement_does_not_drop_the_executable_statement() {
    for mode in [TranslationMode::BestEffort, TranslationMode::Strict] {
        let service = TranslationService::new_sqlglot(vec![])
            .unwrap()
            .with_policy(mode, false);
        let report = service
            .maybe_translate_report(
                ";SELECT 1",
                &SqlDialect::Trino,
                &SqlDialect::DuckDb,
                &SchemaContext::default(),
                &[],
            )
            .await;
        assert_eq!(report.result.unwrap(), "SELECT 1");
        assert_eq!(report.outcome.status, TranslationStatus::Fallback);
    }
}

#[tokio::test]
async fn trailing_comment_is_not_a_second_statement() {
    let service = TranslationService::new_sqlglot(vec![])
        .unwrap()
        .with_policy(TranslationMode::Strict, false);
    let report = service
        .maybe_translate_report(
            "SELECT 1; -- trailing comment",
            &SqlDialect::Trino,
            &SqlDialect::DuckDb,
            &SchemaContext::default(),
            &[],
        )
        .await;
    let sql = report.result.unwrap();
    assert!(sql.contains("SELECT 1"), "{sql}");
    assert!(sql.contains("trailing comment"), "{sql}");
}

#[tokio::test]
async fn statement_trivia_survives_schema_translation_and_same_dialect_fixups() {
    let service = TranslationService::new_sqlglot(vec![])
        .unwrap()
        .with_policy(TranslationMode::Strict, false);
    let schema = SchemaContext {
        tables: std::collections::HashMap::from([(
            "t".into(),
            ColumnMap::from([("x".into(), "INT".into())]),
        )]),
        ..Default::default()
    };
    for (target, schema, fixups) in [
        (SqlDialect::DuckDb, schema, vec![]),
        (
            SqlDialect::Trino,
            SchemaContext::default(),
            vec!["def transform(sql, src, dst): return sql".into()],
        ),
    ] {
        let report = service
            .maybe_translate_report(
                ";SELECT x FROM t; -- trailing comment",
                &SqlDialect::Trino,
                &target,
                &schema,
                &fixups,
            )
            .await;
        let sql = report.result.unwrap();
        assert!(sql.contains('x'), "{sql}");
        assert!(sql.contains("trailing comment"), "{sql}");
        assert_eq!(report.outcome.status, TranslationStatus::Yes);
    }
}
