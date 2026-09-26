//! Separate test process: changing Python's import table must not race other tests.
use pyo3::prelude::*;
use queryflux_core::{
    config::TranslationMode,
    query::{SqlDialect, TranslationReason},
};
use queryflux_translation::{SchemaContext, TranslationService};

#[tokio::test]
async fn import_failure_after_startup_retains_unavailable_reason() {
    let service = TranslationService::new_sqlglot(vec![])
        .unwrap()
        .with_policy(TranslationMode::Strict, false);
    let original = Python::attach(|py| {
        let modules = PyModule::import(py, "sys")
            .unwrap()
            .getattr("modules")
            .unwrap();
        let original = modules.get_item("sqlglot").unwrap().unbind();
        modules.set_item("sqlglot", py.None()).unwrap();
        original
    });
    let report = service
        .maybe_translate_report(
            "SELECT 1",
            &SqlDialect::Trino,
            &SqlDialect::DuckDb,
            &SchemaContext::default(),
            &[],
        )
        .await;
    Python::attach(|py| {
        PyModule::import(py, "sys")
            .unwrap()
            .getattr("modules")
            .unwrap()
            .set_item("sqlglot", original)
            .unwrap();
    });
    assert!(report.result.is_err());
    assert_eq!(
        report.outcome.reason,
        Some(TranslationReason::SqlglotUnavailable)
    );
}
