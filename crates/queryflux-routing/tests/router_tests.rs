/// Unit tests for router implementations (`Header`, `ProtocolBased`, `ClientTags`, `QueryRegex`,
/// `Compound`, `PythonScript`) and `RouterChain`.
///
/// Covers `FrontendProtocol` variants on `ProtocolBasedRouter`, `CompoundCondition` kinds
/// (including `Header` on Trino HTTP + ClickHouse HTTP), and chain fallthrough.
///
/// No engine or HTTP server required — pure routing logic.
/// Run: `cargo test -p queryflux-routing`
use std::collections::HashMap;

use queryflux_auth::AuthContext;
use queryflux_core::{
    config::{CompoundCombineMode, CompoundCondition},
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        compound::CompoundRouter, header::HeaderRouter, protocol_based::ProtocolBasedRouter,
        python_script::PythonScriptRouter, query_regex::QueryRegexRouter, tags::TagsRouter,
    },
    ChainRouteResult, RouterTrait, RoutingDecision,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn trino_session(headers: &[(&str, &str)]) -> SessionContext {
    let extra: HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let user = extra.get("x-trino-user").cloned();
    SessionContext {
        user,
        extra,
        ..Default::default()
    }
}

fn postgres_session() -> SessionContext {
    SessionContext {
        user: Some("alice".to_string()),
        ..Default::default()
    }
}

fn mysql_session(user: Option<&str>) -> SessionContext {
    SessionContext {
        user: user.map(String::from),
        ..Default::default()
    }
}

fn clickhouse_session(headers: &[(&str, &str)]) -> SessionContext {
    let extra: HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.to_lowercase(), (*v).to_string()))
        .collect();
    let user = extra.get("user").cloned();
    let database = extra.get("database").cloned();
    SessionContext {
        user,
        database,
        extra,
        ..Default::default()
    }
}

fn group(name: &str) -> ClusterGroupName {
    ClusterGroupName(name.to_string())
}

// ---------------------------------------------------------------------------
// HeaderRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn header_router_matches() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    let session = trino_session(&[("x-target", "analytics")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("analytics-group")));
}

#[tokio::test]
async fn header_router_value_not_in_mapping() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    let session = trino_session(&[("x-target", "unknown-value")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn header_router_absent() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    let session = trino_session(&[]); // no headers
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn header_router_non_trino_session() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    // HeaderRouter only applies to TrinoHttp sessions
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

// ---------------------------------------------------------------------------
// ProtocolBasedRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn protocol_router_trino_http() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: Some(group("pg-group")),
        mysql_wire: None,
        clickhouse_http: None,
        flight_sql: None,
        snowflake_http: None,
        snowflake_sql_api: None,
        mcp: None,
    };
    let session = trino_session(&[]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("trino-group")));
}

#[tokio::test]
async fn protocol_router_postgres_wire() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: Some(group("pg-group")),
        mysql_wire: None,
        clickhouse_http: None,
        flight_sql: None,
        snowflake_http: None,
        snowflake_sql_api: None,
        mcp: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("pg-group")));
}

#[tokio::test]
async fn protocol_router_unconfigured() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: None, // not configured
        mysql_wire: None,
        clickhouse_http: None,
        flight_sql: None,
        snowflake_http: None,
        snowflake_sql_api: None,
        mcp: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn protocol_router_mysql_wire() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: None,
        mysql_wire: Some(group("mysql-group")),
        clickhouse_http: None,
        flight_sql: None,
        snowflake_http: None,
        snowflake_sql_api: None,
        mcp: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &mysql_session(Some("root")),
            &FrontendProtocol::MySqlWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("mysql-group")));
}

#[tokio::test]
async fn protocol_router_clickhouse_http() {
    let router = ProtocolBasedRouter {
        trino_http: None,
        postgres_wire: None,
        mysql_wire: None,
        clickhouse_http: Some(group("ch-group")),
        flight_sql: None,
        snowflake_http: None,
        snowflake_sql_api: None,
        mcp: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &clickhouse_session(&[]),
            &FrontendProtocol::ClickHouseHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("ch-group")));
}

#[tokio::test]
async fn protocol_router_flight_sql() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: Some(group("pg-group")),
        mysql_wire: Some(group("mysql-group")),
        clickhouse_http: Some(group("ch-group")),
        flight_sql: Some(group("sf-analytics")),
        snowflake_http: None,
        snowflake_sql_api: None,
        mcp: None,
    };
    // `ProtocolBasedRouter` routes on frontend protocol only (session is ignored).
    let result = router
        .route(
            "SELECT 1",
            &trino_session(&[]),
            &FrontendProtocol::FlightSql,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("sf-analytics")));
}

#[tokio::test]
async fn protocol_router_snowflake_http() {
    let router = ProtocolBasedRouter {
        trino_http: None,
        postgres_wire: None,
        mysql_wire: None,
        clickhouse_http: None,
        flight_sql: None,
        snowflake_http: Some(group("sf-wire")),
        snowflake_sql_api: None,
        mcp: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &mysql_session(Some("u")),
            &FrontendProtocol::SnowflakeHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("sf-wire")));
}

#[tokio::test]
async fn protocol_router_snowflake_sql_api() {
    let router = ProtocolBasedRouter {
        trino_http: None,
        postgres_wire: None,
        mysql_wire: None,
        clickhouse_http: None,
        flight_sql: None,
        snowflake_http: None,
        snowflake_sql_api: Some(group("sf-rest")),
        mcp: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &mysql_session(Some("u")),
            &FrontendProtocol::SnowflakeSqlApi,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("sf-rest")));
}

// ---------------------------------------------------------------------------
// TagsRouter
// ---------------------------------------------------------------------------

/// Build a Trino session with tags pre-extracted from a `X-Trino-Client-Tags`-style header value.
/// Each comma-separated element becomes a key-only tag (None value), matching Trino wire semantics.
fn trino_session_with_client_tags(tags_header: &str) -> SessionContext {
    let tags = tags_header
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| (t.to_string(), None))
        .collect();
    SessionContext {
        extra: HashMap::from([("x-trino-client-tags".to_string(), tags_header.to_string())]),
        tags,
        ..Default::default()
    }
}

fn tag_rule(tags: &[(&str, Option<&str>)], target: &str) -> queryflux_core::config::TagRoutingRule {
    queryflux_core::config::TagRoutingRule {
        tags: tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect(),
        target_group: target.to_string(),
    }
}

#[tokio::test]
async fn tags_trino_key_only_match() {
    let router = TagsRouter::new(vec![tag_rule(&[("premium", None)], "premium-group")]);
    let session = trino_session_with_client_tags("premium");
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("premium-group")));
}

#[tokio::test]
async fn tags_kv_match_postgres() {
    let router = TagsRouter::new(vec![tag_rule(
        &[("team", Some("analytics"))],
        "analytics-group",
    )]);
    let (tags, _) = queryflux_core::tags::parse_query_tags("team:analytics");
    let session = SessionContext {
        user: Some("alice".to_string()),
        tags,
        ..Default::default()
    };
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::PostgresWire, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("analytics-group")));
}

#[tokio::test]
async fn tags_kv_wrong_value_no_match() {
    let router = TagsRouter::new(vec![tag_rule(
        &[("team", Some("analytics"))],
        "analytics-group",
    )]);
    // x-trino-client-tags treats the whole "team:eng" string as a single key-only tag.
    let session = trino_session_with_client_tags("team:eng");
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn tags_no_tags_no_match() {
    let router = TagsRouter::new(vec![tag_rule(&[("premium", None)], "premium-group")]);
    let session = trino_session(&[]); // no tags header
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn tags_and_logic_partial_no_match() {
    let router = TagsRouter::new(vec![tag_rule(
        &[("team", Some("eng")), ("env", Some("prod"))],
        "prod-eng",
    )]);
    let (tags, _) = queryflux_core::tags::parse_query_tags("team:eng,env:staging");
    let session = SessionContext {
        tags,
        ..Default::default()
    };
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::MySqlWire, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

// ---------------------------------------------------------------------------
// QueryRegexRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_regex_match() {
    let router = QueryRegexRouter::new(vec![(
        r"(?i)\bSELECT\b.*\bFROM\b.*\borders\b".to_string(),
        "orders-group".to_string(),
    )]);
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT id FROM orders WHERE total > 100",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("orders-group")));
}

#[tokio::test]
async fn query_regex_no_match() {
    let router = QueryRegexRouter::new(vec![(
        r"(?i)\borders\b".to_string(),
        "orders-group".to_string(),
    )]);
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT id FROM customers",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn query_regex_first_match_wins() {
    let router = QueryRegexRouter::new(vec![
        (r"(?i)\borders\b".to_string(), "orders-group".to_string()),
        (
            r"(?i)\bcustomers\b".to_string(),
            "customers-group".to_string(),
        ),
    ]);
    let session = trino_session(&[]);
    // SQL matches both, first rule should win
    let result = router
        .route(
            "SELECT * FROM orders JOIN customers ON orders.cust_id = customers.id",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("orders-group")));
}

#[tokio::test]
async fn query_regex_invalid_regex_skipped() {
    // Invalid regex is silently skipped at construction; valid rule still works
    let router = QueryRegexRouter::new(vec![
        (r"[invalid regex(".to_string(), "bad-group".to_string()),
        (r"(?i)\borders\b".to_string(), "orders-group".to_string()),
    ]);
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT * FROM orders",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("orders-group")));
}

#[tokio::test]
async fn query_regex_matches_under_postgres_protocol() {
    let router = QueryRegexRouter::new(vec![(
        r"(?i)\bcustomers\b".to_string(),
        "cust-group".to_string(),
    )]);
    let result = router
        .route(
            "SELECT * FROM customers",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("cust-group")));
}

// ---------------------------------------------------------------------------
// CompoundRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compound_all_both_match() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![
            CompoundCondition::Protocol {
                protocol: "trinoHttp".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "premium-group".to_string(),
    );
    let session = trino_session(&[("x-trino-user", "alice")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("premium-group")));
}

#[tokio::test]
async fn compound_all_one_fails() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![
            CompoundCondition::Protocol {
                protocol: "trinoHttp".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "premium-group".to_string(),
    );
    // Protocol matches but user is "bob", not "alice"
    let session = trino_session(&[("x-trino-user", "bob")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn compound_any_first_matches() {
    let router = CompoundRouter::new(
        CompoundCombineMode::Any,
        vec![
            CompoundCondition::Protocol {
                protocol: "trinoHttp".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "any-group".to_string(),
    );
    // Protocol matches (TrinoHttp), user doesn't matter
    let session = trino_session(&[("x-trino-user", "bob")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("any-group")));
}

#[tokio::test]
async fn compound_any_none_match() {
    let router = CompoundRouter::new(
        CompoundCombineMode::Any,
        vec![
            CompoundCondition::Protocol {
                protocol: "mysqlWire".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "any-group".to_string(),
    );
    // Protocol is TrinoHttp (not mysqlWire), user is "bob" (not "alice")
    let session = trino_session(&[("x-trino-user", "bob")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn compound_client_tag_condition() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::ClientTag {
            tag: "team-a".to_string(),
        }],
        "team-a-group".to_string(),
    );
    let session = trino_session(&[("x-trino-client-tags", "team-a,priority=high")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("team-a-group")));
}

#[tokio::test]
async fn compound_query_regex_condition() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::QueryRegex {
            regex: r"(?i)\blineitem\b".to_string(),
        }],
        "lineitem-group".to_string(),
    );
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT * FROM lineitem LIMIT 10",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("lineitem-group")));
}

#[tokio::test]
async fn compound_header_condition_trino_http() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::Header {
            header_name: "X-Route-Key".to_string(),
            header_value: "batch".to_string(),
        }],
        "batch-group".to_string(),
    );
    let session = trino_session(&[("x-route-key", "batch")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("batch-group")));
}

#[tokio::test]
async fn compound_header_condition_clickhouse_http() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::Header {
            header_name: "X-Custom-Route".to_string(),
            header_value: "olap".to_string(),
        }],
        "olap-group".to_string(),
    );
    let session = clickhouse_session(&[("x-custom-route", "olap")]);
    let result = router
        .route(
            "SELECT 1",
            &session,
            &FrontendProtocol::ClickHouseHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("olap-group")));
}

#[tokio::test]
async fn compound_user_matches_mysql_session_user() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::User {
            username: "reporter".to_string(),
        }],
        "mysql-users".to_string(),
    );
    let result = router
        .route(
            "SELECT 1",
            &mysql_session(Some("reporter")),
            &FrontendProtocol::MySqlWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("mysql-users")));
}

#[tokio::test]
async fn compound_user_prefers_auth_context_over_session_header() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::User {
            username: "alice".to_string(),
        }],
        "auth-wins".to_string(),
    );
    let session = trino_session(&[("x-trino-user", "bob")]);
    let auth = AuthContext {
        user: "alice".to_string(),
        groups: vec![],
        roles: vec![],
        raw_token: None,
        ..Default::default()
    };
    let result = router
        .route(
            "SELECT 1",
            &session,
            &FrontendProtocol::TrinoHttp,
            Some(&auth),
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("auth-wins")));
}

#[tokio::test]
async fn compound_protocol_mysql_wire() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::Protocol {
            protocol: "mysqlWire".to_string(),
        }],
        "mysql-only".to_string(),
    );
    let result = router
        .route(
            "SELECT 1",
            &mysql_session(None),
            &FrontendProtocol::MySqlWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("mysql-only")));
}

#[tokio::test]
async fn compound_protocol_clickhouse_http() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::Protocol {
            protocol: "clickhouseHttp".to_string(),
        }],
        "ch-only".to_string(),
    );
    let result = router
        .route(
            "SELECT 1",
            &clickhouse_session(&[]),
            &FrontendProtocol::ClickHouseHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("ch-only")));
}

#[tokio::test]
async fn compound_protocol_flight_sql() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::Protocol {
            protocol: "flightSql".to_string(),
        }],
        "flight-only".to_string(),
    );
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::FlightSql,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::Route(group("flight-only")));
}

#[tokio::test]
async fn compound_empty_conditions_never_matches() {
    let router = CompoundRouter::new(CompoundCombineMode::All, vec![], "empty-target".to_string());
    let session = trino_session(&[("x-trino-user", "alice")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

#[tokio::test]
async fn compound_only_invalid_regex_conditions_never_matches() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::QueryRegex {
            regex: "[unclosed".to_string(),
        }],
        "bad-regex-target".to_string(),
    );
    let result = router
        .route(
            "SELECT * FROM orders",
            &trino_session(&[]),
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, RoutingDecision::NoMatch);
}

// ---------------------------------------------------------------------------
// PythonScriptRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn python_script_sees_ctx_protocol() {
    let script = r#"
def route(query, ctx):
    if ctx.get("protocol") == "trinoHttp":
        return "g-trino"
    return None
"#;
    let router = PythonScriptRouter::new(script.to_string());
    let session = trino_session(&[]);
    let out = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(out, RoutingDecision::Route(group("g-trino")));
}

#[tokio::test]
async fn python_script_auth_in_ctx() {
    let script = r#"
def route(query, ctx):
    auth = ctx.get("auth") or {}
    if auth.get("user") == "alice" and "admins" in (auth.get("groups") or []):
        return "admin-group"
    return None
"#;
    let router = PythonScriptRouter::new(script.to_string());
    let session = trino_session(&[]);
    let auth = AuthContext {
        user: "alice".to_string(),
        groups: vec!["admins".to_string()],
        roles: vec![],
        raw_token: None,
        ..Default::default()
    };
    let out = router
        .route(
            "SELECT 1",
            &session,
            &FrontendProtocol::TrinoHttp,
            Some(&auth),
        )
        .await
        .unwrap();
    assert_eq!(out, RoutingDecision::Route(group("admin-group")));
}

#[tokio::test]
async fn python_script_returns_none_passes() {
    let script = r#"
def route(query, ctx):
    return None
"#;
    let router = PythonScriptRouter::new(script.to_string());
    let session = trino_session(&[]);
    let out = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(out, RoutingDecision::NoMatch);
}

// ---------------------------------------------------------------------------
// RouterChain
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chain_first_router_matches() {
    let chain = RouterChain::new(
        vec![
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("analytics".to_string(), group("analytics-group"))]),
            )),
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("other".to_string(), group("other-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-group", "analytics")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("analytics-group")));
    assert!(!trace.used_fallback);
}

#[tokio::test]
async fn chain_fallback_when_no_match() {
    let chain = RouterChain::new(
        vec![Box::new(HeaderRouter::new(
            "x-group".to_string(),
            HashMap::from([("analytics".to_string(), group("analytics-group"))]),
        ))],
        group("fallback-group"),
    );
    let session = trino_session(&[]); // no header → no match
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("fallback-group")));
    assert!(trace.used_fallback);
}

#[tokio::test]
async fn chain_trace_records_all_decisions() {
    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::new(vec![(
                r"(?i)\borders\b".to_string(),
                "orders-group".to_string(),
            )])),
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("analytics".to_string(), group("analytics-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    // Neither router matches
    let session = trino_session(&[]);
    let (_, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    // Both routers were evaluated and recorded
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched);
    assert!(!trace.decisions[1].matched);
    assert!(trace.used_fallback);
}

#[tokio::test]
async fn chain_second_router_matches() {
    let chain = RouterChain::new(
        vec![
            Box::new(HeaderRouter::new(
                "x-first".to_string(),
                HashMap::from([("yes".to_string(), group("first-group"))]),
            )),
            Box::new(HeaderRouter::new(
                "x-second".to_string(),
                HashMap::from([("yes".to_string(), group("second-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    // Only second header present
    let session = trino_session(&[("x-second", "yes")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("second-group")));
    assert!(!trace.used_fallback);
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched); // first router missed
    assert!(trace.decisions[1].matched); // second router hit
}

#[tokio::test]
async fn chain_short_circuits_after_first_match() {
    let chain = RouterChain::new(
        vec![
            Box::new(HeaderRouter::new(
                "x-priority".to_string(),
                HashMap::from([("high".to_string(), group("priority-group"))]),
            )),
            Box::new(HeaderRouter::new(
                "x-other".to_string(),
                HashMap::from([("low".to_string(), group("other-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-priority", "high"), ("x-other", "low")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("priority-group")));
    assert!(!trace.used_fallback);
    assert_eq!(
        trace.decisions.len(),
        1,
        "second router must not run after first match"
    );
    assert!(trace.decisions[0].matched);
}

#[tokio::test]
async fn chain_regex_then_header_first_wins_on_both_match() {
    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::new(vec![(
                r"^SELECT".to_string(),
                "regex-group".to_string(),
            )])),
            Box::new(HeaderRouter::new(
                "x-route".to_string(),
                HashMap::from([("batch".to_string(), group("batch-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-route", "batch")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("regex-group")));
    assert_eq!(trace.decisions.len(), 1);
    assert!(trace.decisions[0].matched);
}

#[tokio::test]
async fn chain_regex_miss_then_header_match() {
    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::new(vec![(
                r"^INSERT".to_string(),
                "insert-group".to_string(),
            )])),
            Box::new(HeaderRouter::new(
                "x-route".to_string(),
                HashMap::from([("api".to_string(), group("api-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-route", "api")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("api-group")));
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched);
    assert!(trace.decisions[1].matched);
}

#[tokio::test]
async fn chain_protocol_based_then_header_fallback() {
    let chain = RouterChain::new(
        vec![
            Box::new(ProtocolBasedRouter {
                trino_http: None,
                postgres_wire: Some(group("pg-from-protocol")),
                mysql_wire: None,
                clickhouse_http: None,
                flight_sql: None,
                snowflake_http: None,
                snowflake_sql_api: None,
                mcp: None,
            }),
            Box::new(HeaderRouter::new(
                "x-tenant".to_string(),
                HashMap::from([("acme".to_string(), group("tenant-acme"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-tenant", "acme")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, ChainRouteResult::Routed(group("tenant-acme")));
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched);
    assert!(trace.decisions[1].matched);
}

#[tokio::test]
async fn chain_deny_short_circuits_before_later_routers() {
    use queryflux_core::config::{QueryRegexRule, RegexRouteAction};

    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::from_rules(vec![QueryRegexRule {
                regex: r"(?i)^\s*DROP".into(),
                target_group: None,
                action: RegexRouteAction::Deny,
                error: Some("DDL denied".into()),
            }])),
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("analytics".to_string(), group("analytics-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-group", "analytics")]);
    let (result, trace) = chain
        .route_with_trace("DROP TABLE t", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(
        result,
        ChainRouteResult::Denied {
            message: "DDL denied".into()
        }
    );
    assert_eq!(trace.denied.as_deref(), Some("DDL denied"));
    assert!(trace.final_group.is_empty());
    assert_eq!(trace.decisions.len(), 1);
    assert!(trace.decisions[0].matched);
}
