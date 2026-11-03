//! A multi-tenant control plane (`controlPlane.tenancy`).
//!
//! Each tenant is a complete control-plane deployment ([`crate::run`]) whose state lives
//! in its own Postgres schema (`persistence.schema`), run inside this process. This
//! process only routes requests to them:
//!
//! - **Public listener** (`adminApi.port`): `/worker/*` for host `<tenant><hostSuffix>`
//!   goes to that tenant. Hubs and workers authenticate there as with any control plane,
//!   against that tenant's enrolled hubs and secret, so a hub of one tenant is never
//!   accepted by another. Nothing else is served: no admin API, no metrics.
//! - **Platform listener** (`tenancy.platformPort`, bearer `$QUERYFLUX_PLATFORM_TOKEN`):
//!   `/platform/tenants` adds and removes tenants, and every other path goes to the
//!   tenant named in `X-QueryFlux-Tenant`: its admin API, worker API, metrics.
//!
//! Tenants are stored in `queryflux_tenants`, in this process's own schema. Every replica
//! starts the tenants listed there and follows changes within [`RECONCILE_EVERY`].

use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use queryflux_core::config::{FrontendsConfig, PersistenceConfig, ProxyConfig};
use queryflux_persistence::postgres::tenants::{PutOutcome, Tenant, TenantRegistry};
use serde_json::json;
use tokio::sync::{oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tower::ServiceExt;

use crate::RunMode;

const RECONCILE_EVERY: Duration = Duration::from_secs(10);
const TENANT_HEADER: &str = "x-queryflux-tenant";
const PLATFORM_TOKEN_ENV: &str = "QUERYFLUX_PLATFORM_TOKEN";

struct Running {
    schema: String,
    router: Router,
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

struct Gateway {
    /// The config every tenant starts from, with its own schema.
    template: ProxyConfig,
    registry: TenantRegistry,
    running: RwLock<HashMap<String, Running>>,
    /// Why a registered tenant is not running, until it starts.
    failed: RwLock<HashMap<String, String>>,
    reconciling: Mutex<()>,
    host_suffix: String,
    platform_token: String,
}

pub(crate) async fn run(config: ProxyConfig) -> Result<()> {
    let tenancy = config
        .queryflux
        .control_plane
        .as_ref()
        .and_then(|c| c.tenancy.clone())
        .context("controlPlane.tenancy is not set")?;
    let PersistenceConfig::Postgres { conn } = &config.queryflux.persistence else {
        anyhow::bail!("controlPlane.tenancy requires Postgres persistence");
    };
    let platform_token = std::env::var(PLATFORM_TOKEN_ENV).unwrap_or_default();
    if platform_token.trim().len() < 16 {
        anyhow::bail!("controlPlane.tenancy: set {PLATFORM_TOKEN_ENV} (at least 16 characters)");
    }
    let registry = TenantRegistry::connect(conn)
        .await
        .context("Tenant registry")?;
    let gateway = Arc::new(Gateway {
        template: config.clone(),
        registry,
        running: RwLock::default(),
        failed: RwLock::default(),
        reconciling: Mutex::default(),
        host_suffix: tenancy.host_suffix.to_ascii_lowercase(),
        platform_token,
    });
    gateway.reconcile().await.context("Starting tenants")?;
    tokio::spawn({
        let gateway = gateway.clone();
        async move {
            let mut every = tokio::time::interval(RECONCILE_EVERY);
            every.tick().await;
            loop {
                every.tick().await;
                if let Err(e) = gateway.reconcile().await {
                    tracing::warn!("Tenant reconcile failed: {e:#}");
                }
            }
        }
    });

    let public_port = config.queryflux.admin_api.port;
    let public = tokio::net::TcpListener::bind(("0.0.0.0", public_port)).await?;
    let platform = tokio::net::TcpListener::bind(("0.0.0.0", tenancy.platform_port)).await?;
    tracing::info!(
        public_port,
        platform_port = tenancy.platform_port,
        host_suffix = %gateway.host_suffix,
        "Multi-tenant control plane listening"
    );
    let (stop_tx, stop_rx) = watch::channel(false);
    let serve = |listener, router: Router| {
        let mut stop = stop_rx.clone();
        axum::serve(listener, router).with_graceful_shutdown(async move {
            let _ = stop.wait_for(|s| *s).await;
        })
    };
    let public = tokio::spawn(serve(public, public_router(gateway.clone())).into_future());
    let platform = tokio::spawn(serve(platform, platform_router(gateway.clone())).into_future());
    crate::startup::shutdown::shutdown_signal().await;
    let _ = stop_tx.send(true);
    let (_, _) = tokio::join!(public, platform);
    let running = std::mem::take(&mut *gateway.running.write().await);
    for (id, t) in running {
        stop(id, t).await;
    }
    tracing::info!("Multi-tenant control plane stopped");
    Ok(())
}

impl Gateway {
    /// Start registered tenants that are not running and stop running ones that are no
    /// longer registered.
    async fn reconcile(&self) -> Result<()> {
        let _one = self.reconciling.lock().await;
        let wanted: HashMap<String, String> = self
            .registry
            .list()
            .await?
            .into_iter()
            .map(|t| (t.id, t.schema))
            .collect();
        let gone: Vec<String> = self
            .running
            .read()
            .await
            .iter()
            .filter(|(id, t)| wanted.get(*id) != Some(&t.schema))
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            if let Some(t) = self.running.write().await.remove(&id) {
                stop(id, t).await;
            }
        }
        self.failed
            .write()
            .await
            .retain(|id, _| wanted.contains_key(id));
        for (id, schema) in wanted {
            if self.running.read().await.contains_key(&id) {
                continue;
            }
            match self.start(&id, &schema).await {
                Ok(t) => {
                    tracing::info!(tenant = %id, schema = %schema, "Tenant started");
                    self.failed.write().await.remove(&id);
                    self.running.write().await.insert(id, t);
                }
                Err(e) => {
                    tracing::error!(tenant = %id, "Tenant failed to start: {e:#}");
                    self.failed.write().await.insert(id, format!("{e:#}"));
                }
            }
        }
        Ok(())
    }

    async fn start(&self, id: &str, schema: &str) -> Result<Running> {
        let config = tenant_config(&self.template, schema);
        let (router_tx, router_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(
            crate::run(
                config,
                RunMode::Tenant {
                    router: router_tx,
                    stop: stop_rx,
                },
            )
            .instrument_tenant(id),
        );
        match router_rx.await {
            Ok(router) => Ok(Running {
                schema: schema.to_string(),
                router,
                stop: stop_tx,
                task,
            }),
            // `run` failed before it had a router to hand over.
            Err(_) => match task.await {
                Ok(Err(e)) => Err(e),
                Ok(Ok(())) => anyhow::bail!("stopped during startup"),
                Err(e) => anyhow::bail!("startup panicked: {e}"),
            },
        }
    }

    async fn dispatch(&self, id: &str, req: Request) -> Response {
        let router = self.running.read().await.get(id).map(|t| t.router.clone());
        match router {
            Some(router) => match router.oneshot(req).await {
                Ok(response) => response,
                Err(never) => match never {},
            },
            None => (StatusCode::NOT_FOUND, "unknown tenant").into_response(),
        }
    }
}

async fn stop(id: String, t: Running) {
    let _ = t.stop.send(true);
    match t.task.await {
        Ok(Ok(())) => tracing::info!(tenant = %id, "Tenant stopped"),
        Ok(Err(e)) => tracing::warn!(tenant = %id, "Tenant stopped with an error: {e:#}"),
        Err(e) => tracing::warn!(tenant = %id, "Tenant task failed: {e}"),
    }
}

trait InstrumentTenant: std::future::Future + Sized {
    fn instrument_tenant(self, id: &str) -> tracing::instrument::Instrumented<Self> {
        tracing::Instrument::instrument(self, tracing::info_span!("tenant", id = %id))
    }
}
impl<F: std::future::Future> InstrumentTenant for F {}

/// A tenant's deployment: the template with the tenant's schema, as a plain control
/// plane with no query listeners (the gateway serves its API).
fn tenant_config(template: &ProxyConfig, schema: &str) -> ProxyConfig {
    let mut config = template.clone();
    if let PersistenceConfig::Postgres { conn } = &mut config.queryflux.persistence {
        conn.schema = Some(schema.to_string());
    }
    if let Some(cp) = config.queryflux.control_plane.as_mut() {
        cp.tenancy = None;
    }
    let mut frontends = FrontendsConfig::default();
    frontends.trino_http.enabled = false;
    config.queryflux.frontends = frontends;
    config
}

/// A tenant id is one DNS label, since it is the first label of the tenant's host.
fn valid_tenant_id(id: &str) -> bool {
    let b = id.as_bytes();
    (1..=63).contains(&b.len())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
        && b[0] != b'-'
        && b[b.len() - 1] != b'-'
}

/// The tenant a request's host names: `<tenant><suffix>`, port ignored.
fn tenant_of_host<'a>(host: &'a str, suffix: &str) -> Option<&'a str> {
    let host = host.rsplit_once(':').map_or(host, |(h, _)| h);
    let id = host.strip_suffix(suffix)?;
    valid_tenant_id(id).then_some(id)
}

fn request_host(req: &Request) -> Option<String> {
    req.headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| req.uri().host())
        .map(str::to_ascii_lowercase)
}

// --- Public listener ---------------------------------------------------------------

fn public_router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .fallback(public_dispatch)
        .with_state(gateway)
}

async fn public_dispatch(State(gw): State<Arc<Gateway>>, req: Request) -> Response {
    if !req.uri().path().starts_with("/worker/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let host = request_host(&req);
    match host
        .as_deref()
        .and_then(|h| tenant_of_host(h, &gw.host_suffix))
    {
        Some(id) => {
            let id = id.to_string();
            gw.dispatch(&id, req).await
        }
        None => (StatusCode::NOT_FOUND, "unknown tenant").into_response(),
    }
}

// --- Platform listener -------------------------------------------------------------

fn platform_router(gateway: Arc<Gateway>) -> Router {
    let authenticated = Router::new()
        .route("/platform/tenants", get(list_tenants))
        .route(
            "/platform/tenants/{id}",
            get(get_tenant).put(put_tenant).delete(delete_tenant),
        )
        .fallback(platform_dispatch)
        .layer(middleware::from_fn_with_state(
            gateway.clone(),
            platform_auth,
        ));
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .merge(authenticated)
        .with_state(gateway)
}

async fn platform_auth(State(gw): State<Arc<Gateway>>, req: Request, next: Next) -> Response {
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if queryflux_frontend::worker_api::secret_matches(token, &gw.platform_token) {
        next.run(req).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

/// Any other path, for the tenant in `X-QueryFlux-Tenant`.
async fn platform_dispatch(State(gw): State<Arc<Gateway>>, req: Request) -> Response {
    let Some(id) = req
        .headers()
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
    else {
        return (StatusCode::BAD_REQUEST, "X-QueryFlux-Tenant is required").into_response();
    };
    let mut req = req;
    req.extensions_mut()
        .insert(queryflux_frontend::admin_access::PlatformCaller);
    // A tenant added through another replica may not be running here yet.
    if !gw.running.read().await.contains_key(&id) && valid_tenant_id(&id) {
        if let Err(e) = gw.reconcile().await {
            tracing::warn!("Tenant reconcile failed: {e:#}");
        }
    }
    gw.dispatch(&id, req).await
}

async fn status(gw: &Gateway, tenant: &Tenant) -> serde_json::Value {
    let running = gw.running.read().await.contains_key(&tenant.id);
    let error = gw.failed.read().await.get(&tenant.id).cloned();
    json!({
        "id": tenant.id,
        "schema": tenant.schema,
        "host": format!("{}{}", tenant.id, gw.host_suffix),
        "running": running,
        "error": error,
    })
}

async fn list_tenants(State(gw): State<Arc<Gateway>>) -> Response {
    match gw.registry.list().await {
        Ok(tenants) => {
            let mut out = Vec::with_capacity(tenants.len());
            for t in &tenants {
                out.push(status(&gw, t).await);
            }
            Json(out).into_response()
        }
        Err(e) => internal(e),
    }
}

async fn get_tenant(State(gw): State<Arc<Gateway>>, Path(id): Path<String>) -> Response {
    match gw.registry.list().await {
        Ok(tenants) => match tenants.iter().find(|t| t.id == id) {
            Some(t) => Json(status(&gw, t).await).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        Err(e) => internal(e),
    }
}

#[derive(serde::Deserialize, Default)]
struct PutTenant {
    /// Defaults to `tenant_<id>` with `-` as `_`.
    schema: Option<String>,
}

/// Register a tenant and start it: its schema is created and migrated, then its worker
/// API answers on its host. `200`/`201` once it runs. A tenant that fails to start stays
/// registered and is retried; `GET` shows the error.
async fn put_tenant(
    State(gw): State<Arc<Gateway>>,
    Path(id): Path<String>,
    body: Option<Json<PutTenant>>,
) -> Response {
    if !valid_tenant_id(&id) {
        return (
            StatusCode::BAD_REQUEST,
            "tenant id must be a lowercase DNS label (a-z, 0-9, -)",
        )
            .into_response();
    }
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let schema = body
        .schema
        .unwrap_or_else(|| format!("tenant_{}", id.replace('-', "_")));
    let check = queryflux_core::config::PostgresPersistenceConfig {
        schema: Some(schema.clone()),
        ..Default::default()
    };
    if let Err(e) = check.pinned_schema() {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    if schema == "public" {
        return (
            StatusCode::BAD_REQUEST,
            "a tenant cannot use schema 'public'",
        )
            .into_response();
    }
    let tenant = Tenant { id, schema };
    let created = match gw.registry.put(&tenant).await {
        Ok(PutOutcome::Created) => true,
        Ok(PutOutcome::Unchanged) => false,
        Ok(PutOutcome::Conflict(why)) => return (StatusCode::CONFLICT, why).into_response(),
        Err(e) => return internal(e),
    };
    if let Err(e) = gw.reconcile().await {
        return internal(e);
    }
    let body = status(&gw, &tenant).await;
    let code = match (body["running"].as_bool(), created) {
        (Some(true), true) => StatusCode::CREATED,
        (Some(true), false) => StatusCode::OK,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(body)).into_response()
}

/// Stop and unregister a tenant. Its schema and data stay: dropping them is up to the
/// caller.
async fn delete_tenant(State(gw): State<Arc<Gateway>>, Path(id): Path<String>) -> Response {
    match gw.registry.delete(&id).await {
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Ok(true) => match gw.reconcile().await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => internal(e),
        },
        Err(e) => internal(e),
    }
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!("Platform API: {e}");
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tenant_comes_from_its_host() {
        let s = ".cp.example.com";
        assert_eq!(tenant_of_host("acme.cp.example.com", s), Some("acme"));
        assert_eq!(tenant_of_host("acme.cp.example.com:443", s), Some("acme"));
        assert_eq!(tenant_of_host("a-1.cp.example.com", s), Some("a-1"));
        // Not one label, not under the suffix, or not a valid id.
        assert_eq!(tenant_of_host("x.acme.cp.example.com", s), None);
        assert_eq!(tenant_of_host("acme.cp.example.org", s), None);
        assert_eq!(tenant_of_host(".cp.example.com", s), None);
        assert_eq!(tenant_of_host("-a.cp.example.com", s), None);
        assert_eq!(tenant_of_host("cp.example.com", s), None);
    }

    #[test]
    fn a_tenant_runs_as_a_plain_control_plane_in_its_schema() {
        let template: ProxyConfig = serde_yaml::from_str(
            "queryflux:\n  role: controlPlane\n  frontends:\n    trinoHttp: { enabled: true, port: 8080 }\n  persistence: { type: postgres, url: postgres://x/y }\n  controlPlane:\n    tenancy: { hostSuffix: .cp.example.com }\n",
        )
        .unwrap();
        let c = tenant_config(&template, "tenant_acme");
        let PersistenceConfig::Postgres { conn } = &c.queryflux.persistence else {
            panic!("postgres")
        };
        assert_eq!(conn.schema.as_deref(), Some("tenant_acme"));
        assert!(c.queryflux.control_plane.unwrap().tenancy.is_none());
        assert!(!crate::startup::tasks::serves_queries(
            &c.queryflux.frontends
        ));
        assert!(!c.queryflux.frontends.trino_http.enabled);
    }
}
