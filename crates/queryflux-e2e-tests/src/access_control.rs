//! Shared OPA stub, catalog, and Postgres-wire helpers for access-control e2e tests.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{extract::State, routing::post, Json, Router};
use queryflux_core::access_config::{
    AccessConnectionConfig, AccessControlConfig, OnMissingSchema, OpaProviderConfig,
};
use queryflux_core::access_model::{ColumnMask, MaskType};
use queryflux_core::catalog::{CatalogProvider, ColumnDef, TableSchema};
use queryflux_core::error::Result as QfResult;
use queryflux_frontend::access_control_guard::OpaAccessGuard;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_postgres::SimpleQueryMessage;

/// Per-table verdict the stub returns for one resource.
#[derive(Clone, Debug, Default)]
pub struct TableVerdict {
    pub allow: bool,
    pub reason: Option<String>,
    pub row_filters: Vec<String>,
    pub column_masks: Vec<ColumnMask>,
    pub ucast: Option<Value>,
}

impl TableVerdict {
    pub fn allow() -> Self {
        Self {
            allow: true,
            ..Self::default()
        }
    }

    pub fn deny(reason: impl Into<String>) -> Self {
        Self {
            allow: false,
            reason: Some(reason.into()),
            ..Self::default()
        }
    }

    pub fn with_filter(mut self, expr: impl Into<String>) -> Self {
        self.row_filters.push(expr.into());
        self
    }

    pub fn with_mask(mut self, mask: ColumnMask) -> Self {
        self.column_masks.push(mask);
        self
    }
}

/// Mutable decision table the in-process OPA stub consults per request.
#[derive(Default)]
pub struct StubState {
    pub deny_tables: HashSet<String>,
    pub row_filters: HashMap<String, String>,
    /// Extra AND-combined filters on top of [`Self::row_filters`].
    pub extra_row_filters: HashMap<String, Vec<String>>,
    pub column_masks: HashMap<String, Vec<ColumnMask>>,
    pub ucast_tables: HashSet<String>,
    /// When set, the stub returns `{"result": {}}` (undefined policy → deny-all).
    pub empty_result: bool,
    /// Full per-user override: `user → table → verdict`. Wins over the maps above.
    pub by_user: HashMap<String, HashMap<String, TableVerdict>>,
    /// Every `input` body the stub has received, in order.
    pub requests: Vec<Value>,
}

impl StubState {
    /// The recorded `input.action` objects whose `operation` is `op`.
    pub fn actions_for(&self, op: &str) -> Vec<Value> {
        self.requests
            .iter()
            .map(|r| r["input"]["action"].clone())
            .filter(|a| a["operation"] == op)
            .collect()
    }

    pub fn deny(&mut self, table: impl Into<String>) {
        self.deny_tables.insert(table.into());
    }

    pub fn filter(&mut self, table: impl Into<String>, expr: impl Into<String>) {
        self.row_filters.insert(table.into(), expr.into());
    }

    pub fn mask(&mut self, table: impl Into<String>, mask: ColumnMask) {
        self.column_masks
            .entry(table.into())
            .or_default()
            .push(mask);
    }

    pub fn user(
        &mut self,
        user: impl Into<String>,
        table: impl Into<String>,
        verdict: TableVerdict,
    ) {
        self.by_user
            .entry(user.into())
            .or_default()
            .insert(table.into(), verdict);
    }
}

fn lookup_verdict<'a>(
    map: &'a HashMap<String, TableVerdict>,
    table: &str,
) -> Option<&'a TableVerdict> {
    map.get(table).or_else(|| {
        let bare = table.rsplit('.').next().unwrap_or(table);
        map.get(bare)
    })
}

fn lookup_map<'a, T>(map: &'a HashMap<String, T>, table: &str) -> Option<&'a T> {
    map.get(table).or_else(|| {
        let bare = table.rsplit('.').next().unwrap_or(table);
        map.get(bare)
    })
}

fn contains_table(set: &HashSet<String>, table: &str) -> bool {
    set.contains(table) || {
        let bare = table.rsplit('.').next().unwrap_or(table);
        set.contains(bare)
    }
}

async fn opa_handler(
    State(state): State<Arc<Mutex<StubState>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let resources = body["input"]["action"]["resources"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let user = body["input"]["identity"]["user"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let mut st = state.lock().unwrap();
    st.requests.push(body.clone());
    if st.empty_result {
        return Json(json!({ "result": {} }));
    }
    let mut out = Vec::new();
    for r in resources {
        let table = r["table"].as_str().unwrap_or("").to_string();
        if let Some(verdict) = st
            .by_user
            .get(&user)
            .and_then(|m| lookup_verdict(m, &table))
        {
            out.push(verdict_json(&table, verdict));
            continue;
        }

        let mut entry = json!({ "table": table, "allow": true });
        if contains_table(&st.deny_tables, &table) {
            entry["allow"] = json!(false);
            entry["reason"] = json!("denied by stub policy");
        } else {
            let mut filters: Vec<Value> = Vec::new();
            if let Some(expr) = lookup_map(&st.row_filters, &table) {
                filters.push(json!({ "expression": expr }));
            }
            if let Some(extra) = lookup_map(&st.extra_row_filters, &table) {
                for expr in extra {
                    filters.push(json!({ "expression": expr }));
                }
            }
            if contains_table(&st.ucast_tables, &table) {
                filters
                    .push(json!({ "ucast": { "type": "eq", "field": "region", "value": "EU" } }));
            }
            if !filters.is_empty() {
                entry["rowFilters"] = json!(filters);
            }
            if let Some(masks) = lookup_map(&st.column_masks, &table) {
                entry["columnMasks"] = serde_json::to_value(masks).unwrap_or(json!([]));
            }
        }
        out.push(entry);
    }
    Json(json!({ "result": { "resources": out } }))
}

fn verdict_json(table: &str, v: &TableVerdict) -> Value {
    let mut entry = json!({ "table": table, "allow": v.allow });
    if let Some(reason) = &v.reason {
        entry["reason"] = json!(reason);
    }
    let mut filters: Vec<Value> = v
        .row_filters
        .iter()
        .map(|e| json!({ "expression": e }))
        .collect();
    if let Some(ucast) = &v.ucast {
        filters.push(json!({ "ucast": ucast }));
    }
    if !filters.is_empty() {
        entry["rowFilters"] = json!(filters);
    }
    if !v.column_masks.is_empty() {
        entry["columnMasks"] = serde_json::to_value(&v.column_masks).unwrap_or(json!([]));
    }
    entry
}

/// Start the stub OPA server and return its base URL + a handle to mutate decisions.
pub async fn start_opa_stub() -> (String, Arc<Mutex<StubState>>) {
    let state = Arc::new(Mutex::new(StubState::default()));
    let app = Router::new()
        .route("/v1/data/queryflux/access", post(opa_handler))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind opa stub");
    let addr = listener.local_addr().expect("stub addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}"), state)
}

pub struct GuardOpts {
    pub on_missing_schema: OnMissingSchema,
    pub fail_open: bool,
    pub session_param_keys: Vec<String>,
    /// Namespaced operations the connection evaluates (default: `table.select` only).
    pub operations: Vec<String>,
}

impl Default for GuardOpts {
    fn default() -> Self {
        Self {
            on_missing_schema: OnMissingSchema::Evaluate,
            fail_open: false,
            session_param_keys: vec![],
            operations: vec!["table.select".to_string()],
        }
    }
}

/// Build a real `OpaAccessGuard` pointed at the stub, evaluating every SELECT.
pub fn build_guard(opa_url: &str) -> Arc<OpaAccessGuard> {
    build_guard_with(opa_url, GuardOpts::default())
}

/// Like [`build_guard`], evaluating exactly the given namespaced operations.
pub fn build_guard_with_operations(opa_url: &str, operations: &[&str]) -> Arc<OpaAccessGuard> {
    build_guard_with(
        opa_url,
        GuardOpts {
            operations: operations.iter().map(|o| o.to_string()).collect(),
            ..GuardOpts::default()
        },
    )
}

pub fn build_guard_with(opa_url: &str, opts: GuardOpts) -> Arc<OpaAccessGuard> {
    let connection = AccessConnectionConfig {
        opa: Some(OpaProviderConfig {
            url: opa_url.to_string(),
            decision_path: "/v1/data/queryflux/access".to_string(),
            timeout_ms: 2_000,
            bearer_token: None,
            client_credentials: None,
        }),
        operations: opts.operations,
        on_missing_schema: opts.on_missing_schema,
        fail_open: opts.fail_open,
        cache_ttl_ms: 0,
        cache_capacity: 100,
        session_param_keys: opts.session_param_keys,
        ..AccessConnectionConfig::default()
    };
    let cfg = AccessControlConfig {
        enabled: true,
        default_connection: Some("default".to_string()),
        connections: HashMap::from([("default".to_string(), connection)]),
        groups: HashMap::new(),
    };
    OpaAccessGuard::try_from_config(&cfg).expect("build access-control guard")
}

pub async fn pg_connect(port: u16) -> tokio_postgres::Client {
    pg_connect_as(port, "testuser").await
}

pub async fn pg_connect_as(port: u16, user: &str) -> tokio_postgres::Client {
    let url = format!("postgresql://{user}@127.0.0.1:{port}/postgres");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("postgres wire connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

pub async fn pg_run(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let messages = client
        .simple_query(sql)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut rows = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let mut vals = Vec::new();
            for i in 0..row.len() {
                vals.push(row.get(i).unwrap_or("").to_string());
            }
            rows.push(vals);
        }
    }
    Ok(rows)
}

pub async fn pg_run_named(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<Vec<HashMap<String, String>>, String> {
    let messages = client
        .simple_query(sql)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut rows = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let mut map = HashMap::new();
            for (i, col) in row.columns().iter().enumerate() {
                map.insert(col.name().to_string(), row.get(i).unwrap_or("").to_string());
            }
            rows.push(map);
        }
    }
    Ok(rows)
}

pub fn mask(column: &str, mask_type: MaskType) -> ColumnMask {
    ColumnMask {
        column: column.to_string(),
        mask_type,
        value: None,
        expression: None,
    }
}

pub fn constant_mask(column: &str, value: &str) -> ColumnMask {
    ColumnMask {
        column: column.to_string(),
        mask_type: MaskType::Constant,
        value: Some(value.to_string()),
        expression: None,
    }
}

pub fn custom_mask(column: &str, expression: &str) -> ColumnMask {
    ColumnMask {
        column: column.to_string(),
        mask_type: MaskType::Custom,
        value: None,
        expression: Some(expression.to_string()),
    }
}

/// In-memory [`CatalogProvider`] keyed by bare table name.
pub struct MapCatalog {
    tables: HashMap<String, TableSchema>,
}

impl MapCatalog {
    pub fn new(tables: Vec<TableSchema>) -> Arc<Self> {
        Arc::new(Self {
            tables: tables.into_iter().map(|t| (t.table.clone(), t)).collect(),
        })
    }
}

#[async_trait]
impl CatalogProvider for MapCatalog {
    async fn list_catalogs(&self) -> QfResult<Vec<String>> {
        Ok(vec![])
    }
    async fn list_databases(&self, _catalog: &str) -> QfResult<Vec<String>> {
        Ok(vec![])
    }
    async fn list_tables(&self, _catalog: &str, _database: &str) -> QfResult<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }
    async fn get_table_schema(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> QfResult<Option<TableSchema>> {
        let bare = table.rsplit('.').next().unwrap_or(table);
        let name_matches =
            |key: &str| -> bool { key == table || key.rsplit('.').next().unwrap_or(key) == bare };
        let catalog_matches = |schema: &TableSchema| -> bool {
            schema.catalog == catalog && schema.database == database
        };

        // Prefer an entry whose declared catalog/database agree with the request — see
        // the identical disambiguation in `queryflux_catalog::StaticCatalogProvider`.
        if let Some(schema) = self
            .tables
            .iter()
            .find(|(key, schema)| name_matches(key) && catalog_matches(schema))
            .map(|(_, schema)| schema)
        {
            return Ok(Some(schema.clone()));
        }
        Ok(self
            .tables
            .iter()
            .find(|(key, _)| name_matches(key))
            .map(|(_, schema)| schema.clone()))
    }
}

pub fn table_schema(name: &str, cols: &[(&str, &str)]) -> TableSchema {
    TableSchema {
        catalog: String::new(),
        database: String::new(),
        table: name.to_string(),
        columns: cols
            .iter()
            .map(|(n, t)| ColumnDef {
                name: (*n).to_string(),
                data_type: (*t).to_string(),
                nullable: true,
            })
            .collect(),
    }
}

pub fn customers_schema() -> TableSchema {
    table_schema(
        "customers",
        &[
            ("id", "INTEGER"),
            ("name", "VARCHAR"),
            ("region", "VARCHAR"),
            ("ssn", "VARCHAR"),
            ("email", "VARCHAR"),
            ("hired", "DATE"),
        ],
    )
}

pub fn orders_schema() -> TableSchema {
    table_schema(
        "orders",
        &[
            ("id", "INTEGER"),
            ("customer_id", "INTEGER"),
            ("amount", "INTEGER"),
            ("region", "VARCHAR"),
        ],
    )
}

pub fn payroll_schema() -> TableSchema {
    table_schema(
        "payroll",
        &[
            ("id", "INTEGER"),
            ("name", "VARCHAR"),
            ("salary", "INTEGER"),
        ],
    )
}

pub async fn seed_customers(client: &tokio_postgres::Client) {
    pg_run(
        client,
        "CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            region VARCHAR,
            ssn VARCHAR,
            email VARCHAR,
            hired DATE
        )",
    )
    .await
    .expect("create customers");
    pg_run(
        client,
        "INSERT INTO customers VALUES
            (1, 'Ana', 'EU', '111-22-3333', 'ana@ex.com', DATE '2020-06-15'),
            (2, 'Ben', 'US', '444-55-6666', 'ben@ex.com', DATE '2021-03-01'),
            (3, 'Cam', 'EU', '777-88-9999', 'cam@ex.com', DATE '2019-12-31')",
    )
    .await
    .expect("insert customers");
}

pub async fn seed_orders(client: &tokio_postgres::Client) {
    pg_run(
        client,
        "CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            amount INTEGER,
            region VARCHAR
        )",
    )
    .await
    .expect("create orders");
    pg_run(
        client,
        "INSERT INTO orders VALUES
            (10, 1, 50, 'EU'),
            (11, 1, 150, 'EU'),
            (12, 2, 200, 'US'),
            (13, 3, 80, 'EU')",
    )
    .await
    .expect("insert orders");
}

pub async fn seed_payroll(client: &tokio_postgres::Client) {
    pg_run(
        client,
        "CREATE TABLE payroll (id INTEGER, name VARCHAR, salary INTEGER)",
    )
    .await
    .expect("create payroll");
    pg_run(
        client,
        "INSERT INTO payroll VALUES (1, 'Ana', 120000), (2, 'Ben', 95000)",
    )
    .await
    .expect("insert payroll");
}
