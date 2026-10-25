//! Who may call the admin API, and what they did.
//!
//! A caller authenticates one of three ways:
//! - **Basic auth** with the admin user (`adminApi.username`/`password`, or the password
//!   changed through the API). It has every scope. Off with `adminApi.basicAuth: false`.
//! - **A bearer token** from `POST /admin/tokens`, with the scopes it was created with.
//! - **The platform**: a gateway in this process that has already authenticated the
//!   caller marks the request with [`PlatformCaller`] (a multi-tenant control plane's
//!   platform listener). It has every scope unless `X-QueryFlux-Scopes` narrows them, and
//!   may name who it acts for in `X-QueryFlux-Actor`. Nobody else can set either.
//!
//! Every route needs one [`Scope`] ([`required_scope`]). Every write is recorded in the
//! audit log (`GET /admin/audit`).

use std::sync::Arc;

use axum::extract::{Extension, Path, Query, Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use queryflux_auth::AdminCredentialsManager;
use queryflux_persistence::admin_access::{AdminAuditEntry, AdminToken};
use queryflux_persistence::AdminStore;
use serde::Deserialize;
use tracing::warn;

/// Marks a request whose caller a gateway in this process authenticated as the platform.
/// Request extensions cannot come from the network, so only in-process code can set it.
#[derive(Debug, Clone, Copy)]
pub struct PlatformCaller;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Every `GET`.
    Read,
    /// Day-to-day operations: sites and hubs, cache invalidation, cancelling queries,
    /// enabling or disabling a cluster.
    Operate,
    /// Every other change to config.
    ConfigWrite,
    /// Admin tokens and the admin password.
    Credentials,
}

impl Scope {
    pub const ALL: [Scope; 4] = [
        Scope::Read,
        Scope::Operate,
        Scope::ConfigWrite,
        Scope::Credentials,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Operate => "operate",
            Scope::ConfigWrite => "config:write",
            Scope::Credentials => "credentials",
        }
    }

    pub fn parse(s: &str) -> Option<Scope> {
        Scope::ALL
            .into_iter()
            .find(|scope| scope.as_str() == s.trim())
    }
}

/// The scope a request needs.
pub fn required_scope(method: &Method, path: &str) -> Scope {
    if path.starts_with("/admin/tokens") || path.starts_with("/admin/auth/change-password") {
        return Scope::Credentials;
    }
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return Scope::Read;
    }
    let operations = [
        "/admin/sites",
        "/admin/cache",
        "/admin/queries/",
        "/admin/clusters/",
    ];
    if operations.iter().any(|p| path.starts_with(p)) {
        Scope::Operate
    } else {
        Scope::ConfigWrite
    }
}

/// The authenticated caller, available to handlers as a request extension.
#[derive(Debug, Clone)]
pub struct Caller {
    /// `basic:<user>`, `token:<id>` or `platform`.
    pub principal: String,
    pub scopes: Vec<Scope>,
    pub actor: Option<String>,
}

pub struct AccessState {
    pub store: Option<Arc<dyn AdminStore>>,
    pub creds: Arc<AdminCredentialsManager>,
    pub basic_auth: bool,
}

const TOKEN_PREFIX: &str = "qfa_";
const MAX_ACTOR_LEN: usize = 200;

/// Authenticate, check the route's scope, run the request, and audit it if it writes.
pub async fn authorize(
    State(st): State<Arc<AccessState>>,
    mut req: Request,
    next: Next,
) -> Response {
    let platform = req.extensions().get::<PlatformCaller>().is_some();
    let caller = match authenticate(&st, req.headers(), platform).await {
        Ok(caller) => caller,
        Err(response) => return response,
    };
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let need = required_scope(&method, &path);
    if !caller.scopes.contains(&need) {
        return (
            StatusCode::FORBIDDEN,
            format!("this needs the '{}' scope", need.as_str()),
        )
            .into_response();
    }
    req.extensions_mut().insert(caller.clone());
    let response = next.run(req).await;
    if need != Scope::Read {
        if let Some(store) = &st.store {
            let entry = AdminAuditEntry {
                id: 0,
                at: chrono::Utc::now(),
                principal: caller.principal,
                actor: caller.actor,
                method: method.to_string(),
                path,
                status: response.status().as_u16(),
            };
            if let Err(e) = store.record_admin_audit(&entry).await {
                warn!("Admin audit log write failed: {e}");
            }
        }
    }
    response
}

async fn authenticate(
    st: &AccessState,
    headers: &header::HeaderMap,
    platform: bool,
) -> Result<Caller, Response> {
    let text = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
    };
    if platform {
        let scopes = match text("x-queryflux-scopes") {
            None => Scope::ALL.to_vec(),
            Some(list) => list
                .split(',')
                .map(|s| Scope::parse(s).ok_or(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|bad| {
                    (StatusCode::BAD_REQUEST, format!("unknown scope '{bad}'")).into_response()
                })?,
        };
        let actor = text("x-queryflux-actor").map(|a| a.chars().take(MAX_ACTOR_LEN).collect());
        return Ok(Caller {
            principal: "platform".into(),
            scopes,
            actor,
        });
    }
    let auth = text(header::AUTHORIZATION.as_str()).unwrap_or("");
    if let Some(token) = auth.strip_prefix("Bearer ") {
        if let (true, Some(store)) = (token.trim().starts_with(TOKEN_PREFIX), &st.store) {
            let hash = crate::worker_api::enrollment_token_hash(token);
            match store.use_admin_token(&hash).await {
                Ok(Some(t)) => {
                    return Ok(Caller {
                        principal: format!("token:{}", t.id),
                        scopes: t.scopes.iter().filter_map(|s| Scope::parse(s)).collect(),
                        actor: None,
                    })
                }
                Ok(None) => warn!("Admin API: unknown or revoked token"),
                Err(e) => {
                    warn!("Admin API: token lookup failed: {e}");
                    return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
                }
            }
        }
    } else if let Some((user, password)) = parse_basic(auth) {
        if st.basic_auth && st.creds.verify(&user, &password).await {
            return Ok(Caller {
                principal: format!("basic:{user}"),
                scopes: Scope::ALL.to_vec(),
                actor: None,
            });
        }
        warn!(user, "Admin API: invalid credentials");
    } else {
        warn!("Admin API: missing or malformed Authorization header");
    }
    let mut response = (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    if st.basic_auth {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static(r#"Basic realm="QueryFlux Admin", charset="UTF-8""#),
        );
    }
    Err(response)
}

fn parse_basic(header: &str) -> Option<(String, String)> {
    use base64::Engine;
    let encoded = header.strip_prefix("Basic ")?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let decoded = String::from_utf8(bytes).ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

// --- Tokens and audit ------------------------------------------------------------------

pub fn routes() -> Router<Arc<AccessState>> {
    Router::new()
        .route("/admin/tokens", get(list_tokens).post(create_token))
        .route("/admin/tokens/{id}", delete(revoke_token))
        .route("/admin/audit", get(list_audit))
}

#[allow(clippy::result_large_err)]
fn store(st: &AccessState) -> Result<&Arc<dyn AdminStore>, Response> {
    st.store.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, "no admin store configured").into_response()
    })
}

fn internal(e: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

async fn list_tokens(State(st): State<Arc<AccessState>>) -> Response {
    let store = match store(&st) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.list_admin_tokens().await {
        Ok(tokens) => Json(tokens).into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct CreateToken {
    name: String,
    scopes: Vec<String>,
}

/// Create a token. It is in the response only: the store keeps its SHA-256. A caller can
/// only grant scopes it has.
async fn create_token(
    State(st): State<Arc<AccessState>>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<CreateToken>,
) -> Response {
    let store = match store(&st) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let name = req.name.trim();
    if name.is_empty() || name.len() > 100 {
        return (StatusCode::BAD_REQUEST, "name must be 1 to 100 characters").into_response();
    }
    let mut scopes = Vec::new();
    for s in &req.scopes {
        match Scope::parse(s) {
            Some(scope) if !caller.scopes.contains(&scope) => {
                return (
                    StatusCode::FORBIDDEN,
                    format!("you cannot grant '{}', which you lack", scope.as_str()),
                )
                    .into_response()
            }
            Some(scope) if !scopes.contains(&scope) => scopes.push(scope),
            Some(_) => {}
            None => {
                return (StatusCode::BAD_REQUEST, format!("unknown scope '{s}'")).into_response()
            }
        }
    }
    if scopes.is_empty() {
        return (StatusCode::BAD_REQUEST, "scopes must not be empty").into_response();
    }
    let secret = format!(
        "{TOKEN_PREFIX}{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let token = AdminToken {
        id: uuid::Uuid::new_v4().to_string(),
        name: name.to_string(),
        token_hash: crate::worker_api::enrollment_token_hash(&secret),
        scopes: scopes.iter().map(|s| s.as_str().to_string()).collect(),
        created_by: match &caller.actor {
            Some(actor) => format!("{} for {actor}", caller.principal),
            None => caller.principal.clone(),
        },
        created_at: chrono::Utc::now(),
        last_used_at: None,
        revoked_at: None,
    };
    if let Err(e) = store.create_admin_token(&token).await {
        return internal(e);
    }
    let mut body = serde_json::to_value(&token).unwrap_or_default();
    body["token"] = serde_json::Value::String(secret);
    (StatusCode::CREATED, Json(body)).into_response()
}

async fn revoke_token(State(st): State<Arc<AccessState>>, Path(id): Path<String>) -> Response {
    let store = match store(&st) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match store.revoke_admin_token(&id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct AuditQuery {
    limit: Option<i64>,
    before: Option<i64>,
}

/// Admin API writes, newest first. Page with `before` = the last entry's id.
async fn list_audit(State(st): State<Arc<AccessState>>, Query(q): Query<AuditQuery>) -> Response {
    let store = match store(&st) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    match store.list_admin_audit(limit, q.before).await {
        Ok(entries) => Json(entries).into_response(),
        Err(e) => internal(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    #[test]
    fn routes_need_the_right_scope() {
        let get = Method::GET;
        let put = Method::PUT;
        assert_eq!(required_scope(&get, "/admin/config/routing"), Scope::Read);
        assert_eq!(
            required_scope(&put, "/admin/config/routing"),
            Scope::ConfigWrite
        );
        assert_eq!(required_scope(&put, "/admin/sites/eu1"), Scope::Operate);
        assert_eq!(
            required_scope(&Method::DELETE, "/admin/queries/q1"),
            Scope::Operate
        );
        assert_eq!(required_scope(&get, "/admin/tokens"), Scope::Credentials);
        assert_eq!(
            required_scope(&Method::POST, "/admin/auth/change-password"),
            Scope::Credentials
        );
        assert_eq!(required_scope(&get, "/admin/audit"), Scope::Read);
    }

    fn make_app(basic_auth: bool) -> (Router, Arc<dyn AdminStore>) {
        let store: Arc<dyn AdminStore> =
            Arc::new(queryflux_persistence::in_memory::InMemoryPersistence::new());
        let st = Arc::new(AccessState {
            store: Some(store.clone()),
            creds: Arc::new(AdminCredentialsManager::new(
                "admin".into(),
                "pw".into(),
                None,
                false,
            )),
            basic_auth,
        });
        let ok = || async { "ok" };
        let app = Router::new()
            .route("/admin/config/routing", get(ok).put(ok))
            .merge(routes().with_state(st.clone()))
            .layer(axum::middleware::from_fn_with_state(st, authorize));
        (app, store)
    }

    async fn call(
        app: &Router,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
        platform: bool,
    ) -> (StatusCode, serde_json::Value) {
        let mut req = Request::builder().method(method).uri(path);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let mut req = req
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        if platform {
            req.extensions_mut().insert(PlatformCaller);
        }
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or_default())
    }

    const ADMIN: (&str, &str) = ("authorization", "Basic YWRtaW46cHc="); // admin:pw

    #[tokio::test]
    async fn a_token_gets_only_its_scopes_until_revoked() {
        let (app, _) = make_app(true);
        let (status, created) = call(
            &app,
            "POST",
            "/admin/tokens",
            &[ADMIN],
            r#"{"name":"ci","scopes":["read"]}"#,
            false,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let bearer = format!("Bearer {}", created["token"].as_str().unwrap());
        let auth = [("authorization", bearer.as_str())];
        let read = call(&app, "GET", "/admin/config/routing", &auth, "", false).await;
        assert_eq!(read.0, StatusCode::OK);
        let write = call(&app, "PUT", "/admin/config/routing", &auth, "", false).await;
        assert_eq!(write.0, StatusCode::FORBIDDEN);
        // It cannot mint itself more.
        let more = r#"{"name":"x","scopes":["config:write"]}"#;
        let mint = call(&app, "POST", "/admin/tokens", &auth, more, false).await;
        assert_eq!(mint.0, StatusCode::FORBIDDEN);

        let id = created["id"].as_str().unwrap();
        let path = format!("/admin/tokens/{id}");
        let revoked = call(&app, "DELETE", &path, &[ADMIN], "", false).await;
        assert_eq!(revoked.0, StatusCode::NO_CONTENT);
        let after = call(&app, "GET", "/admin/config/routing", &auth, "", false).await;
        assert_eq!(after.0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn only_the_platform_names_an_actor_and_every_write_is_audited() {
        let (app, store) = make_app(true);
        let actor = ("x-queryflux-actor", "user:alice");
        call(&app, "PUT", "/admin/config/routing", &[actor], "", true).await;
        call(
            &app,
            "PUT",
            "/admin/config/routing",
            &[ADMIN, actor],
            "",
            false,
        )
        .await;
        call(&app, "GET", "/admin/config/routing", &[ADMIN], "", false).await;
        let log = store.list_admin_audit(10, None).await.unwrap();
        let seen: Vec<_> = log
            .iter()
            .map(|e| (e.principal.as_str(), e.actor.as_deref(), e.method.as_str()))
            .collect();
        assert_eq!(
            seen,
            vec![
                ("basic:admin", None, "PUT"),
                ("platform", Some("user:alice"), "PUT"),
            ]
        );
    }

    #[tokio::test]
    async fn the_platform_can_narrow_its_scopes_and_basic_auth_can_be_off() {
        let (app, _) = make_app(true);
        let narrowed = [("x-queryflux-scopes", "read")];
        let w = call(&app, "PUT", "/admin/config/routing", &narrowed, "", true).await;
        assert_eq!(w.0, StatusCode::FORBIDDEN);
        let r = call(&app, "GET", "/admin/config/routing", &narrowed, "", true).await;
        assert_eq!(r.0, StatusCode::OK);
        // Without the in-process marker the headers mean nothing.
        let outside = call(&app, "GET", "/admin/config/routing", &narrowed, "", false).await;
        assert_eq!(outside.0, StatusCode::UNAUTHORIZED);

        let (off, _) = make_app(false);
        let basic = call(&off, "GET", "/admin/config/routing", &[ADMIN], "", false).await;
        assert_eq!(basic.0, StatusCode::UNAUTHORIZED);
    }
}
