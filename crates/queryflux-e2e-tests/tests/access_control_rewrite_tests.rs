//! Complex OPA e2e coverage: row filters, every named column-mask type, identity
//! (alice vs bob), joins / CTEs / subqueries, and fail-closed rewrite edges.
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test access_control_rewrite_tests`

use queryflux_core::access_config::OnMissingSchema;
use queryflux_core::access_model::MaskType;
use queryflux_e2e_tests::access_control::{
    build_guard, build_guard_with, constant_mask, custom_mask, customers_schema, mask,
    orders_schema, payroll_schema, pg_connect, pg_connect_as, pg_run, pg_run_named, seed_customers,
    seed_orders, seed_payroll, start_opa_stub, GuardOpts, MapCatalog, TableVerdict,
};
use queryflux_e2e_tests::harness::ProtocolWireHarness;
use serde_json::json;

fn customers_catalog() -> std::sync::Arc<MapCatalog> {
    MapCatalog::new(vec![customers_schema(), orders_schema(), payroll_schema()])
}

async fn harness_with_catalog(opa_url: &str) -> ProtocolWireHarness {
    let guard = build_guard(opa_url);
    ProtocolWireHarness::new_with_access_control_and_catalog(Some(guard), customers_catalog())
        .await
        .expect("harness")
}

fn parse_ids(rows: &[Vec<String>]) -> Vec<i32> {
    rows.iter()
        .map(|r| r[0].parse::<i32>().expect("id is an int"))
        .collect()
}

#[tokio::test]
async fn two_and_combined_row_filters() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("orders", "amount > 100");
        st.extra_row_filters
            .insert("orders".into(), vec!["region = 'US'".into()]);
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;

    let rows = pg_run(&client, "SELECT id FROM orders ORDER BY id")
        .await
        .expect("select");
    assert_eq!(
        parse_ids(&rows),
        vec![12],
        "only the US order with amount > 100 must survive both AND filters"
    );
}

#[tokio::test]
async fn insert_is_not_row_filtered_but_later_select_is() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("orders", "amount > 100");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;

    pg_run(&client, "INSERT INTO orders VALUES (99, 1, 10, 'EU')")
        .await
        .expect("INSERT is statement.other / table.insert — not rewritten");

    let rows = pg_run(&client, "SELECT id FROM orders WHERE id = 99")
        .await
        .expect("select");
    assert!(
        rows.is_empty(),
        "row filter must hide the just-inserted amount=10 row from SELECT"
    );
}

#[tokio::test]
async fn show_last_4_masks_ssn() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT id, ssn FROM customers ORDER BY id")
        .await
        .expect("select");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], "****3333");
    assert_eq!(rows[1][1], "****6666");
    assert_eq!(rows[2][1], "****9999");
}

#[tokio::test]
async fn show_first_4_masks_ssn() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowFirst4));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT ssn FROM customers WHERE id = 1")
        .await
        .expect("select");
    assert_eq!(rows[0][0], "111-****");
}

#[tokio::test]
async fn null_mask_returns_sql_null() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::Null));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT ssn FROM customers WHERE id = 1")
        .await
        .expect("select");
    assert_eq!(rows[0][0], "", "NULL comes through the wire as empty");
}

#[tokio::test]
async fn constant_mask_replaces_email() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", constant_mask("email", "REDACTED"));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT email FROM customers ORDER BY id")
        .await
        .expect("select");
    assert!(rows.iter().all(|r| r[0] == "REDACTED"));
}

#[tokio::test]
async fn redact_mask_replaces_alphanumerics() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::Redact));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT ssn FROM customers WHERE id = 1")
        .await
        .expect("select");
    // Postgres-wire client -> DuckDB backend: `render_mask` sees the source (Postgres)
    // dialect and must use the `g` flag so every alphanumeric is replaced, not just the
    // first — a partially-redacted SSN is still a data exposure.
    assert_eq!(
        rows[0][0], "xxx-xx-xxxx",
        "REDACT must replace every alphanumeric, got {:?}",
        rows[0][0]
    );
}

#[tokio::test]
async fn custom_mask_uppercases_name() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", custom_mask("name", "upper(name)"));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT name FROM customers ORDER BY id")
        .await
        .expect("select");
    assert_eq!(
        rows.iter().map(|r| r[0].as_str()).collect::<Vec<_>>(),
        vec!["ANA", "BEN", "CAM"]
    );
}

#[tokio::test]
async fn date_show_year_truncates_hired() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("hired", MaskType::DateShowYear));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT hired FROM customers WHERE id = 1")
        .await
        .expect("select");
    assert!(
        rows[0][0].starts_with("2020"),
        "date_trunc('year', …) must keep the year, got {:?}",
        rows[0][0]
    );
    assert!(
        !rows[0][0].contains("06-15") && !rows[0][0].contains("06/15"),
        "month/day must be truncated away, got {:?}",
        rows[0][0]
    );
}

#[tokio::test]
async fn hash_mask_hides_ssn() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::Hash));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT ssn FROM customers WHERE id = 1")
        .await
        .expect("select");
    let hashed = &rows[0][0];
    assert_ne!(hashed, "111-22-3333");
    assert!(
        hashed.len() >= 32,
        "hash digest should be a long hex/string, got {hashed:?}"
    );
}

#[tokio::test]
async fn filter_applies_to_unmasked_values_then_mask_is_projected() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "ssn = '111-22-3333'");
        st.mask("customers", mask("ssn", MaskType::ShowLast4));
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(&client, "SELECT id, ssn FROM customers")
        .await
        .expect("select");
    assert_eq!(
        rows.len(),
        1,
        "policy WHERE is spliced onto the base scan (raw SSN)"
    );
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "****3333");

    // Client predicates sit *outside* the scan-site view, so they see the masked
    // projection — not the raw column the policy filter used.
    let by_raw = pg_run(
        &client,
        "SELECT id FROM customers WHERE ssn = '111-22-3333'",
    )
    .await
    .expect("select");
    assert!(
        by_raw.is_empty(),
        "client WHERE ssn = raw value must miss: the outer query sees the mask"
    );
    let by_mask = pg_run(&client, "SELECT id FROM customers WHERE ssn = '****3333'")
        .await
        .expect("select");
    assert_eq!(
        parse_ids(&by_mask),
        vec![1],
        "client WHERE on the masked literal must match the projected column"
    );
}

#[tokio::test]
async fn select_star_returns_masked_column() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run_named(&client, "SELECT * FROM customers WHERE id = 1")
        .await
        .expect("select *");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ssn").map(String::as_str), Some("****3333"));
    assert_eq!(rows[0].get("name").map(String::as_str), Some("Ana"));
}

#[tokio::test]
async fn cte_propagates_row_filter_and_mask() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.mask("customers", mask("ssn", MaskType::ShowLast4));
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "WITH people AS (SELECT id, ssn FROM customers)
         SELECT id, ssn FROM people ORDER BY id",
    )
    .await
    .expect("cte select");
    assert_eq!(parse_ids(&rows), vec![1, 3]);
    assert_eq!(rows[0][1], "****3333");
    assert_eq!(rows[1][1], "****9999");
}

#[tokio::test]
async fn from_subquery_is_rewritten() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("customers", "region = 'EU'");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT id FROM (SELECT id, region FROM customers) t ORDER BY id",
    )
    .await
    .expect("subquery");
    assert_eq!(parse_ids(&rows), vec![1, 3]);
}

#[tokio::test]
async fn in_subquery_filters_both_tables() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.filter("orders", "amount > 100");
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;

    // EU customers are 1 (Ana) and 3 (Cam). Orders with amount > 100: 11 (Ana/150), 12 (Ben/200).
    // Intersection: only order 11.
    let rows = pg_run(
        &client,
        "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers) ORDER BY id",
    )
    .await
    .expect("in subquery");
    assert_eq!(parse_ids(&rows), vec![11]);
}

#[tokio::test]
async fn exists_subquery_sees_filtered_customers() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("customers", "region = 'EU'");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;

    let rows = pg_run(
        &client,
        "SELECT id FROM orders o
         WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)
         ORDER BY id",
    )
    .await
    .expect("exists");
    // Ben's US order 12 must drop because Ben is not an EU customer.
    assert_eq!(parse_ids(&rows), vec![10, 11, 13]);
}

#[tokio::test]
async fn join_applies_per_table_filters() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.filter("orders", "amount > 100");
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;

    let rows = pg_run(
        &client,
        "SELECT o.id FROM customers c
         JOIN orders o ON c.id = o.customer_id
         ORDER BY o.id",
    )
    .await
    .expect("join");
    assert_eq!(parse_ids(&rows), vec![11]);
}

#[tokio::test]
async fn self_join_rewrites_both_scan_sites() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("customers", "region = 'EU'");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT a.id, b.id FROM customers a
         JOIN customers b ON a.region = b.region
         WHERE a.id < b.id
         ORDER BY a.id, b.id",
    )
    .await
    .expect("self-join");
    assert_eq!(rows, vec![vec!["1".to_string(), "3".to_string()]]);
}

#[tokio::test]
async fn union_all_masks_both_branches() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT ssn FROM customers WHERE id = 1
         UNION ALL
         SELECT ssn FROM customers WHERE id = 2",
    )
    .await
    .expect("union");
    let ssns: Vec<_> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ssns.contains(&"****3333"), "got {ssns:?}");
    assert!(ssns.contains(&"****6666"), "got {ssns:?}");
    assert!(!ssns
        .iter()
        .any(|s| s.contains("111-22") || s.contains("444-55")));
}

#[tokio::test]
async fn join_is_denied_when_one_table_is_denied() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().deny("payroll");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_payroll(&client).await;

    let err = pg_run(
        &client,
        "SELECT c.name, p.salary FROM customers c JOIN payroll p ON c.id = p.id",
    )
    .await
    .expect_err("join touching a denied table must fail");
    let lower = err.to_lowercase();
    assert!(
        lower.contains("denied") || lower.contains("payroll"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn alice_sees_all_customers_and_payroll_bob_is_restricted() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.user("alice", "customers", TableVerdict::allow());
        st.user("alice", "payroll", TableVerdict::allow());
        st.user(
            "bob",
            "customers",
            TableVerdict::allow()
                .with_filter("region = 'EU'")
                .with_mask(mask("ssn", MaskType::ShowLast4)),
        );
        st.user(
            "bob",
            "payroll",
            TableVerdict::deny("bob cannot read payroll"),
        );
    }

    let h = harness_with_catalog(&opa_url).await;
    seed_customers(&pg_connect(h.postgres_port).await).await;
    seed_payroll(&pg_connect(h.postgres_port).await).await;

    let alice = pg_connect_as(h.postgres_port, "alice").await;
    let alice_customers = pg_run(&alice, "SELECT id, ssn FROM customers ORDER BY id")
        .await
        .expect("alice customers");
    assert_eq!(parse_ids(&alice_customers), vec![1, 2, 3]);
    assert_eq!(
        alice_customers[1][1], "444-55-6666",
        "alice must see raw SSN"
    );
    let alice_payroll = pg_run(&alice, "SELECT id FROM payroll ORDER BY id")
        .await
        .expect("alice payroll");
    assert_eq!(parse_ids(&alice_payroll), vec![1, 2]);

    let bob = pg_connect_as(h.postgres_port, "bob").await;
    let bob_customers = pg_run(&bob, "SELECT id, ssn FROM customers ORDER BY id")
        .await
        .expect("bob customers");
    assert_eq!(parse_ids(&bob_customers), vec![1, 3]);
    assert_eq!(bob_customers[0][1], "****3333");
    assert_eq!(bob_customers[1][1], "****9999");
    let err = pg_run(&bob, "SELECT id FROM payroll")
        .await
        .expect_err("bob must not read payroll");
    assert!(err.to_lowercase().contains("denied") || err.to_lowercase().contains("payroll"));
}

#[tokio::test]
async fn ucast_row_filter_is_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().ucast_tables.insert("customers".into());

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let err = pg_run(&client, "SELECT id FROM customers")
        .await
        .expect_err("ucast filters are unimplemented");
    let lower = err.to_lowercase();
    assert!(
        lower.contains("ucast") || lower.contains("structured"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn ucast_plus_expression_still_rejected() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.user(
            "testuser",
            "customers",
            TableVerdict {
                allow: true,
                reason: None,
                row_filters: vec!["region = 'EU'".into()],
                column_masks: vec![],
                ucast: Some(json!({"type": "eq", "field": "region", "value": "EU"})),
            },
        );
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let err = pg_run(&client, "SELECT id FROM customers")
        .await
        .expect_err("ucast present even alongside an expression must deny");
    assert!(err.to_lowercase().contains("ucast") || err.to_lowercase().contains("structured"));
}

#[tokio::test]
async fn mask_without_schema_fails_rewrite() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::Null));

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let err = pg_run(&client, "SELECT * FROM customers")
        .await
        .expect_err("masking SELECT * without catalog schema must fail closed");
    let lower = err.to_lowercase();
    assert!(
        lower.contains("enumerat")
            || lower.contains("rewrite")
            || lower.contains("schema")
            || lower.contains("access"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn named_column_mask_works_without_catalog() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT name, region, ssn FROM customers ORDER BY id",
    )
    .await
    .expect("named columns must be enough to apply a mask without a catalog");
    assert_eq!(rows[0][2], "****3333");
    assert_eq!(rows[1][2], "****6666");
}

#[tokio::test]
async fn masked_in_subquery_with_self_join_without_catalog() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.mask("customers", mask("ssn", MaskType::ShowLast4));
    }

    let guard = build_guard(&opa_url);
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT a.id, b.id FROM customers a
         JOIN customers b ON a.id < b.id
         WHERE a.id IN (SELECT id FROM customers)
         ORDER BY a.id, b.id",
    )
    .await
    .expect("IN subquery id must remain resolvable after mask rewrite");
    assert_eq!(rows, vec![vec!["1".to_string(), "3".to_string()]]);
}

#[tokio::test]
async fn on_missing_schema_deny_rejects_unresolved_select() {
    let (opa_url, _stub) = start_opa_stub().await;
    let guard = build_guard_with(
        &opa_url,
        GuardOpts {
            on_missing_schema: OnMissingSchema::Deny,
            ..GuardOpts::default()
        },
    );
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let err = pg_run(&client, "SELECT * FROM customers")
        .await
        .expect_err("onMissingSchema=deny must reject SELECT * without schema");
    let lower = err.to_lowercase();
    assert!(
        lower.contains("schema") || lower.contains("unresolved") || lower.contains("access"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn opa_empty_result_is_deny_all() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().empty_result = true;

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let err = pg_run(&client, "SELECT id FROM customers")
        .await
        .expect_err("undefined OPA result must deny");
    assert!(err.to_lowercase().contains("denied") || err.to_lowercase().contains("policy"));
}

#[tokio::test]
async fn missing_custom_expression_fails_mask_render() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().mask(
        "customers",
        queryflux_core::access_model::ColumnMask {
            column: "ssn".into(),
            mask_type: MaskType::Custom,
            value: None,
            expression: None,
        },
    );

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let err = pg_run(&client, "SELECT ssn FROM customers")
        .await
        .expect_err("CUSTOM without expression must deny");
    assert!(
        err.to_lowercase().contains("custom") || err.to_lowercase().contains("expression"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn fail_open_allows_when_opa_is_unreachable() {
    let guard = build_guard_with(
        "http://127.0.0.1:1",
        GuardOpts {
            fail_open: true,
            ..GuardOpts::default()
        },
    );
    let h = ProtocolWireHarness::new_with_access_control(Some(guard))
        .await
        .expect("harness");
    let client = pg_connect(h.postgres_port).await;
    pg_run(&client, "CREATE TABLE t (id INTEGER)")
        .await
        .expect("create");
    pg_run(&client, "INSERT INTO t VALUES (1)")
        .await
        .expect("insert");

    let rows = pg_run(&client, "SELECT id FROM t")
        .await
        .expect("fail-open must let the query through");
    assert_eq!(parse_ids(&rows), vec![1]);
}

#[tokio::test]
async fn window_order_sees_masked_ssn() {
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT id, ssn, row_number() OVER (ORDER BY ssn) AS rn
         FROM customers
         ORDER BY rn",
    )
    .await
    .expect("window");
    // Masked values sort as ****3333, ****6666, ****9999 — same relative order as raw last-4.
    let ids: Vec<_> = rows.iter().map(|r| r[0].as_str()).collect();
    let ssns: Vec<_> = rows.iter().map(|r| r[1].as_str()).collect();
    assert_eq!(ids, vec!["1", "2", "3"]);
    assert_eq!(ssns, vec!["****3333", "****6666", "****9999"]);
}

#[tokio::test]
async fn multiple_masks_on_one_table() {
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.mask("customers", mask("ssn", MaskType::ShowLast4));
        st.mask("customers", constant_mask("email", "hidden"));
        st.mask("customers", custom_mask("name", "upper(name)"));
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT name, ssn, email FROM customers WHERE id = 2",
    )
    .await
    .expect("select");
    assert_eq!(rows[0][0], "BEN");
    assert_eq!(rows[0][1], "****6666");
    assert_eq!(rows[0][2], "hidden");
}

// ---------------------------------------------------------------------------
// Additional complex-SQL coverage: outer joins, multi-table joins with distinct
// policies, aggregation, CASE expressions, correlated subqueries, set-operation
// dedup, and pagination — all through the real rewrite -> translate -> DuckDB path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn left_join_preserves_outer_semantics_through_row_filter() {
    // Filtering customers to EU must not turn a LEFT JOIN into an INNER JOIN: Ben's
    // (US, filtered out) order still has to appear, with NULL customer columns — exactly
    // what a real database's row-level security does, because the filter applies at the
    // scan, before the join, not as a post-join predicate.
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("customers", "region = 'EU'");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;

    let rows = pg_run(
        &client,
        "SELECT o.id, c.name FROM orders o
         LEFT JOIN customers c ON o.customer_id = c.id
         ORDER BY o.id",
    )
    .await
    .expect("left join");
    assert_eq!(
        rows,
        vec![
            vec!["10".to_string(), "Ana".to_string()],
            vec!["11".to_string(), "Ana".to_string()],
            vec!["12".to_string(), "".to_string()], // Ben (US) filtered out — NULL, row kept
            vec!["13".to_string(), "Cam".to_string()],
        ]
    );
}

#[tokio::test]
async fn three_way_join_applies_three_independent_policies() {
    // customers: row-filtered. orders: row-filtered (a different predicate). payroll:
    // column-masked (no filter). All three policies must apply simultaneously within one
    // rewrite pass, each at its own scan site.
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.filter("orders", "amount > 100");
        st.mask("payroll", mask("salary", MaskType::Null));
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;
    seed_payroll(&client).await;

    // customers ∩ EU = {1, 3}; orders ∩ amount>100 = {11 (cust 1), 12 (cust 2)};
    // payroll has rows for {1, 2}. Three-way join on customer id 1 is the only survivor:
    // it's EU, has an order > 100 (11), and has a payroll row (masked to NULL).
    let rows = pg_run_named(
        &client,
        "SELECT c.id AS cid, o.id AS oid, p.salary AS salary
         FROM customers c
         JOIN orders o ON c.id = o.customer_id
         JOIN payroll p ON c.id = p.id
         ORDER BY c.id",
    )
    .await
    .expect("three-way join");
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one surviving row, got {rows:?}"
    );
    assert_eq!(rows[0].get("cid").map(String::as_str), Some("1"));
    assert_eq!(rows[0].get("oid").map(String::as_str), Some("11"));
    assert_eq!(rows[0].get("salary").map(String::as_str), Some(""));
}

#[tokio::test]
async fn group_by_having_aggregates_only_filtered_rows() {
    // Row filter excludes order 12 (US); GROUP BY / HAVING must aggregate over what's
    // left, not the full table.
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;

    // EU orders: (10, cust1, 50), (11, cust1, 150), (13, cust3, 80).
    let rows = pg_run_named(
        &client,
        "SELECT customer_id AS cust, COUNT(*) AS cnt, SUM(amount) AS total
         FROM orders
         GROUP BY customer_id
         HAVING COUNT(*) > 1
         ORDER BY customer_id",
    )
    .await
    .expect("group by / having");
    // Only customer 1 has more than one EU order (10 and 11); customer 3 (one order) and
    // customer 2 (no EU orders at all) must both be excluded by HAVING.
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert_eq!(rows[0].get("cust").map(String::as_str), Some("1"));
    assert_eq!(rows[0].get("cnt").map(String::as_str), Some("2"));
    assert_eq!(rows[0].get("total").map(String::as_str), Some("200"));
}

#[tokio::test]
async fn group_by_masked_date_column_groups_on_the_masked_value() {
    // GROUP BY the DATE_SHOW_YEAR-masked `hired` column must group on the truncated
    // (masked) value, not the raw date underneath — proving the mask is applied before
    // grouping, at the scan, not as a display-only transform on already-grouped rows.
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.mask("customers", mask("hired", MaskType::DateShowYear));
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    // EU customers: Ana (hired 2020-06-15), Cam (hired 2019-12-31) — distinct years, so
    // two groups, each truncated to January 1st of its year.
    let rows = pg_run(
        &client,
        "SELECT hired, COUNT(*) FROM customers GROUP BY hired ORDER BY hired",
    )
    .await
    .expect("group by masked date");
    assert_eq!(rows.len(), 2, "got {rows:?}");
    // The truncated value is January 1st of the year. Its *type* is up to the engine —
    // DuckDB returns a DATE from `date_trunc('year', <date>)` in 1.4 but a TIMESTAMP (with a
    // `T00:00:00` suffix) from 1.5 on — so match the date part and not what follows it.
    assert!(rows[0][0].starts_with("2019-01-01"), "{rows:?}");
    assert!(rows[1][0].starts_with("2020-01-01"), "{rows:?}");
    assert_eq!(rows[0][1], "1");
    assert_eq!(rows[1][1], "1");
}

#[tokio::test]
async fn case_expression_evaluates_against_the_masked_projection() {
    // A CASE expression built on top of a masked column must see the masked value, not
    // the raw one underneath — proves masking happens at the scan-site projection, so
    // every downstream expression (not just a bare SELECT of the column) is affected.
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", mask("ssn", MaskType::ShowLast4));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    // Every raw SSN in the seed data starts with a 3-digit prefix (e.g. "111-22-3333");
    // no masked value (always "****....") can ever match a "starts with digits" pattern.
    let rows = pg_run(
        &client,
        "SELECT id, CASE WHEN ssn LIKE '1__-%' THEN 'raw-leaked' ELSE 'masked' END AS flag
         FROM customers ORDER BY id",
    )
    .await
    .expect("case expression");
    assert!(
        rows.iter().all(|r| r[1] == "masked"),
        "a CASE built on the masked column must never see the raw SSN, got {rows:?}"
    );
}

#[tokio::test]
async fn correlated_scalar_subquery_counts_only_filtered_orders() {
    // A correlated scalar subquery in the SELECT list must see the row-filtered version
    // of the table it references, the same as any other reference to that table.
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.filter("orders", "amount > 60");
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;
    seed_orders(&client).await;

    // EU customers: 1, 3. Orders with amount > 60: 11 (cust 1, 150), 12 (cust 2, 200),
    // 13 (cust 3, 80) — order 10 (cust 1, 50) is filtered out.
    let rows = pg_run(
        &client,
        "SELECT c.id,
                (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count
         FROM customers c
         ORDER BY c.id",
    )
    .await
    .expect("correlated scalar subquery");
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()], // only order 11 (order 10 filtered out)
            vec!["3".to_string(), "1".to_string()], // order 13
        ]
    );
}

#[tokio::test]
async fn plain_union_dedupes_across_masked_rows() {
    // CONSTANT-masks every row's email to the same literal, then UNIONs two overlapping
    // selections. Plain UNION (not UNION ALL) must dedupe against the *masked* output —
    // proving the mask applies before set-operation dedup, not after.
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock()
        .unwrap()
        .mask("customers", constant_mask("email", "hidden"));

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "SELECT email FROM customers WHERE id IN (1, 2)
         UNION
         SELECT email FROM customers WHERE id IN (2, 3)",
    )
    .await
    .expect("union");
    assert_eq!(
        rows,
        vec![vec!["hidden".to_string()]],
        "every row masks to the same constant, so plain UNION must collapse to one row"
    );
}

#[tokio::test]
async fn order_by_limit_offset_paginates_the_filtered_set() {
    // LIMIT/OFFSET must operate on the row-filtered, ordered set — not on the full table
    // with the filter applied afterward, which would silently return the wrong page.
    let (opa_url, stub) = start_opa_stub().await;
    stub.lock().unwrap().filter("orders", "region = 'EU'");

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_orders(&client).await;

    // EU orders in id order: 10, 11, 13 (12 is US, filtered out). OFFSET 1 LIMIT 1 must
    // land on 11, not on 12 (which would appear only if the filter were applied too late).
    let rows = pg_run(
        &client,
        "SELECT id FROM orders ORDER BY id LIMIT 1 OFFSET 1",
    )
    .await
    .expect("paginated select");
    assert_eq!(parse_ids(&rows), vec![11]);
}

#[tokio::test]
async fn three_level_nested_ctes_propagate_filter_and_mask() {
    // A CTE built on a CTE built on a CTE (three levels deep) must still see the
    // row-filtered, column-masked base table at the bottom.
    let (opa_url, stub) = start_opa_stub().await;
    {
        let mut st = stub.lock().unwrap();
        st.filter("customers", "region = 'EU'");
        st.mask("customers", mask("ssn", MaskType::ShowLast4));
    }

    let h = harness_with_catalog(&opa_url).await;
    let client = pg_connect(h.postgres_port).await;
    seed_customers(&client).await;

    let rows = pg_run(
        &client,
        "WITH base AS (SELECT id, ssn FROM customers),
              mid AS (SELECT id, ssn FROM base),
              top AS (SELECT id, ssn FROM mid)
         SELECT id, ssn FROM top ORDER BY id",
    )
    .await
    .expect("three-level nested cte");
    assert_eq!(parse_ids(&rows), vec![1, 3]);
    assert_eq!(rows[0][1], "****3333");
    assert_eq!(rows[1][1], "****9999");
}
