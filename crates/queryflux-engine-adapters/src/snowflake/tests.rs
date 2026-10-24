use std::io::{Read, Write};
use std::net::TcpListener;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use futures::StreamExt;

use super::*;
use crate::EngineConfigParseable;

const LOGIN_OK: &str = r#"{"success":true,"data":{"token":"session-token"}}"#;

// BOOLEAN is "1"/"0", DATE is days since epoch, TIMESTAMP_NTZ is "secs.nanos".
const QUERY_OK: &str = r#"{"success":true,"data":{"queryId":"01ab","rowset":[["1","alice","12.34","1","20642","1700000000.000000000","{\"a\":1}"],["2",null,null,"0",null,null,null]],"rowtype":[{"name":"ID","nullable":false,"precision":38,"scale":0,"type":"fixed"},{"name":"NAME","nullable":true,"length":16,"type":"text"},{"name":"PRICE","nullable":true,"precision":10,"scale":2,"type":"fixed"},{"name":"ACTIVE","nullable":false,"type":"boolean"},{"name":"D","nullable":true,"type":"date"},{"name":"TS","nullable":true,"scale":9,"type":"timestamp_ntz"},{"name":"V","nullable":true,"type":"variant"}],"queryResultFormat":"json"}}"#;

/// Answers each accepted connection with the next canned body.
fn mock_snowflake(responses: Vec<&'static str>) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for body in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.read(&mut [0_u8; 16384]);
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
        }
    });
    format!("http://{addr}")
}

fn json_config(endpoint: &str) -> serde_json::Value {
    serde_json::json!({
        "account": "demo-account",
        "endpoint": endpoint,
        "warehouse": "COMPUTE_WH",
        "authType": "basic",
        "authUsername": "svc",
        "authPassword": "secret",
    })
}

#[tokio::test]
async fn executes_query_and_maps_types_to_arrow() {
    let endpoint = mock_snowflake(vec![LOGIN_OK, QUERY_OK]);
    let config = SnowflakeConfig::from_json(&json_config(&endpoint), "sf").unwrap();
    let adapter = SnowflakeAdapter::new(
        ClusterName("sf".into()),
        ClusterGroupName("g".into()),
        config,
    )
    .unwrap();

    let slot = BackendQueryIdSlot::new();
    let exec = adapter
        .execute_as_arrow(
            "SELECT ...",
            &queryflux_core::session::SessionContext::default(),
            &queryflux_auth::QueryCredentials::ServiceAccount,
            &Default::default(),
            &Default::default(),
            Default::default(),
            &slot,
        )
        .await
        .unwrap();
    assert!(slot.get().is_some(), "request id published for cancel");

    let batches: Vec<_> = exec.stream.collect().await;
    let batch = batches.into_iter().next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let col = |i: usize| batch.column(i).as_any();
    let ids = col(0).downcast_ref::<Int64Array>().unwrap();
    assert_eq!((ids.value(0), ids.value(1)), (1, 2));
    let names = col(1).downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "alice");
    assert!(names.is_null(1));
    let price = col(2).downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(price.value_as_string(0), "12.34");
    assert!(price.is_null(1));
    let active = col(3).downcast_ref::<BooleanArray>().unwrap();
    assert!(active.value(0) && !active.value(1));
    let d = col(4).downcast_ref::<Date32Array>().unwrap();
    assert_eq!(d.value(0), 20642);
    let ts = col(5).downcast_ref::<TimestampMicrosecondArray>().unwrap();
    assert_eq!(ts.value(0), 1_700_000_000_000_000);
    let v = col(6).downcast_ref::<StringArray>().unwrap();
    assert_eq!(v.value(0), r#"{"a":1}"#);

    // Streaming finished → canceller removed.
    assert!(adapter.cancellers.is_empty());
}

#[test]
fn config_requires_account_and_supported_auth() {
    let mut json = json_config("http://x");
    json.as_object_mut().unwrap().remove("account");
    assert!(SnowflakeConfig::from_json(&json, "sf").is_err());

    let mut json = json_config("http://x");
    json["authType"] = "bearer".into();
    json["authToken"] = "t".into();
    assert!(SnowflakeConfig::from_json(&json, "sf").is_err());

    let mut json = json_config("http://x");
    json["maxWaitSecs"] = 0.into();
    assert!(SnowflakeConfig::from_json(&json, "sf").is_err());
}

#[test]
fn yaml_config_parses() {
    let cfg: ClusterConfig = serde_yaml::from_str(
        r#"
engine: snowflake
account: myorg-myaccount
warehouse: COMPUTE_WH
role: ANALYST
database: ANALYTICS
schema: PUBLIC
maxWaitSecs: 120
auth:
  type: keyPair
  username: SVC
  privateKeyPem: "-----BEGIN PRIVATE KEY-----"
"#,
    )
    .unwrap();
    let parsed = SnowflakeConfig::from_cluster_config(&cfg, "sf").unwrap();
    assert_eq!(parsed.account, "myorg-myaccount");
    assert_eq!(parsed.warehouse.as_deref(), Some("COMPUTE_WH"));
    assert_eq!(parsed.max_wait_secs, 120);
    assert!(matches!(parsed.auth, ClusterAuth::KeyPair { .. }));
}

#[test]
fn quote_ident_escapes_quotes() {
    assert_eq!(quote_ident(r#"my"db"#), r#""my""db""#);
}
