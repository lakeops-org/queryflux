use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use queryflux_auth::BackendIdentityResolver;
use queryflux_config::{yaml::YamlFileConfigProvider, ConfigProvider};
use queryflux_core::query::{ClusterGroupName, ClusterName};
use queryflux_frontend::{
    admin::{
        build_frontends_status, AdminFrontend, RoutingConfigDto as AdminRoutingConfigDto,
        SecurityConfigDto as AdminSecurityConfigDto, TestCatalogProviderFn, TestClusterFn,
    },
    trino_http::state::AppState,
    FrontendListenerTrait,
};
use queryflux_persistence::AdminStore;
use queryflux_translation::TranslationService;
use tracing::info;

mod config_source;
mod guards;
mod history_uplink;
mod live_config;
mod registered_engines;
mod remote_coordination;
mod startup;

use crate::config_source::{
    run_reload_loop, AdminStoreConfigSource, ConfigSource, LiveHandles, PostgresConfigSource,
    ReloadTriggers,
};
use crate::guards::{
    build_access_control_guard, build_auth_provider, build_authorization, build_guard_chains,
    build_guard_chains_from_db_value, load_guard_script_bodies, operators_from_auth,
    validate_group_translation_scripts,
};
use crate::live_config::{
    cluster_configs_from_specs, compile_live_config, AdapterReloadCache, ClusterSpec, CompileMode,
    ConfigIds, LiveConfigInputs,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = match queryflux_cli::run_cli().await? {
        queryflux_cli::CliAction::Exit => return Ok(()),
        queryflux_cli::CliAction::Migrate { config } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "queryflux=info".into()),
                )
                .init();
            let config = YamlFileConfigProvider::new(&config)
                .load()
                .await
                .context("Failed to load config")?;
            queryflux_persistence::run_persistence_migrations(&config.queryflux.persistence)
                .await
                .context("Migration failed")?;
            tracing::info!("Migrations applied successfully");
            return Ok(());
        }
        queryflux_cli::CliAction::Serve { config } => config,
    };

    // Load config before initializing the tracing subscriber so that
    // `otlpEndpoint` from the config file can feed the OTel layer.
    let mut config = YamlFileConfigProvider::new(&config_path)
        .load()
        .await
        .context("Failed to load config")?;

    // Initialize tracing subscriber — with OTel if configured.
    {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "queryflux=info,queryflux_frontend=info".into());

        #[cfg(feature = "otlp")]
        {
            if let Some(endpoint) = &config.queryflux.otlp_endpoint {
                use opentelemetry::trace::TracerProvider;
                use opentelemetry_otlp::WithExportConfig;
                use tracing_subscriber::layer::SubscriberExt;
                use tracing_subscriber::util::SubscriberInitExt;

                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .build()
                    .expect("Failed to create OTLP exporter");
                let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(
                        opentelemetry_sdk::Resource::builder()
                            .with_service_name("queryflux")
                            .build(),
                    )
                    .build();
                let telemetry =
                    tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("queryflux"));
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(tracing_subscriber::fmt::layer())
                    .with(telemetry)
                    .init();
                tracing::info!(endpoint = %endpoint, "OpenTelemetry OTLP tracing enabled");
            } else {
                tracing_subscriber::fmt().with_env_filter(env_filter).init();
            }
        }

        #[cfg(not(feature = "otlp"))]
        {
            tracing_subscriber::fmt().with_env_filter(env_filter).init();
        }
    }

    info!("QueryFlux starting — loaded config from: {}", config_path);

    // --- Role (#255): check the settings that depend on what this process does ---
    if let Err(issues) = config.validate_role() {
        for issue in &issues {
            tracing::error!("Config: {issue}");
        }
        anyhow::bail!("Config has {} role error(s) — aborting", issues.len());
    }
    info!(role = ?config.queryflux.role, "Process role");
    // A worker owns no config: it starts empty and pulls its config from the control
    // plane (`RemoteConfigSource`), serves an ops-only listener instead of the admin API,
    // and skips the config-loading and security checks below, which describe config it
    // does not have.
    let is_worker = config.queryflux.role == queryflux_core::config::Role::Worker;

    let external_address = config
        .queryflux
        .external_address
        .clone()
        .unwrap_or_else(|| "http://localhost:8080".to_string())
        .trim_end_matches('/')
        .to_string();

    // --- Build persistence + metrics stores (must happen before cluster building) ---
    let startup::stores::Stores {
        prometheus,
        persistence,
        metrics,
        mem_store,
        backend,
        distributed_backend,
        coordination,
        snapshot_store,
        worker_registry,
        history_store,
    } = startup::stores::build_stores(&config).await?;
    // Coordination the control plane serves to workers: its own, only when it enforces
    // capacity with it (otherwise dead workers' leases would never expire).
    let served_coordination = coordination.clone();
    // A worker's executing and queued queries live in its own memory, so it sweeps them
    // itself (`local_sweeps`), while capacity leases and engine counts are shared through
    // the control plane with `coordination.mode: controlPlane`.
    let (coordination, local_sweeps) = if is_worker {
        let coord_cfg = config.queryflux.coordination.clone().unwrap_or_default();
        let remote = match (&config.queryflux.control_plane, coord_cfg.mode) {
            (Some(cp), queryflux_core::config::CoordinationMode::ControlPlane) => {
                info!(
                    on_unavailable = ?coord_cfg.on_unavailable,
                    "Capacity limits are shared with the fleet through the control plane"
                );
                Some(remote_coordination::remote_coordination(
                    cp,
                    coord_cfg.on_unavailable,
                ))
            }
            _ => {
                info!("coordination.mode: local — limits apply to this worker alone");
                None
            }
        };
        (remote, None)
    } else {
        (coordination.clone(), coordination)
    };

    // Filled when Postgres loads cluster/group rows — used for query_history FKs on ClusterState.
    let mut cluster_ids_by_name: HashMap<String, i64> = HashMap::new();
    let mut group_ids_by_name: HashMap<String, i64> = HashMap::new();
    // Cluster specs loaded from Postgres; `None` for YAML-only startup.
    let mut startup_cluster_specs: Option<Vec<ClusterSpec>> = None;

    // --- When Postgres is active, load cluster/group config from DB ---
    // Seed YAML `clusters` / `clusterGroups` only for names that are not already in
    // Postgres. Existing DB rows (Studio/admin edits) win and are never overwritten
    // by a ConfigMap/Helm values restart. First boot with an empty DB still seeds
    // from YAML. Omit those maps (or leave them empty) for Studio-only setups.
    if let Some(pg) = &backend {
        if !config.clusters.is_empty() {
            let report = queryflux_persistence::yaml_seed::seed_clusters_from_yaml_if_missing(
                pg.as_ref(),
                &config.clusters,
            )
            .await
            .context("Seed clusters from YAML")?;
            if report.seeded > 0 {
                info!(
                    "Seeded {} cluster definition(s) from YAML into Postgres",
                    report.seeded
                );
            } else if report.existing_before > 0 {
                info!(
                    existing = report.existing_before,
                    "Skipping YAML cluster upsert — Postgres already has these cluster names"
                );
            }
        }
        if !config.cluster_groups.is_empty() {
            let report = queryflux_persistence::yaml_seed::seed_groups_from_yaml_if_missing(
                pg.as_ref(),
                &config.cluster_groups,
            )
            .await
            .context("Seed cluster groups from YAML")?;
            if report.seeded > 0 {
                info!(
                    "Seeded {} cluster group definition(s) from YAML into Postgres",
                    report.seeded
                );
            } else if report.existing_before > 0 {
                info!(
                    existing = report.existing_before,
                    "Skipping YAML group upsert — Postgres already has these group names"
                );
            }
        }

        // Effective config comes from Postgres (YAML above only seeds missing names).
        info!("Loading cluster and group configs from Postgres");
        let db_cluster_records = pg
            .list_cluster_configs()
            .await
            .context("Load cluster configs from DB")?;
        cluster_ids_by_name = db_cluster_records
            .iter()
            .map(|r| (r.name.clone(), r.id))
            .collect();
        // `ClusterConfig` values for validation, startup warnings and credential resolution.
        // Adapters are built from the persisted JSON by `compile_live_config` below.
        let specs: Vec<ClusterSpec> = db_cluster_records
            .iter()
            .map(ClusterSpec::from_record)
            .collect();
        config.clusters = cluster_configs_from_specs(&specs)?;
        startup_cluster_specs = Some(specs);

        let group_records = pg
            .list_group_configs()
            .await
            .context("Load group configs from DB")?;
        group_ids_by_name = group_records
            .iter()
            .map(|r| (r.name.clone(), r.id))
            .collect();
        config.cluster_groups = group_records
            .into_iter()
            .map(|r| Ok((r.name.clone(), r.to_core().map_err(anyhow::Error::msg)?)))
            .collect::<Result<_>>()?;

        // Apply persisted security overrides (`security_settings` / `security_config` key).
        // The migration seeds `{}`; that is not an override — keep YAML.
        if let Ok(Some(v)) = pg.get_proxy_setting("security_config").await {
            if !queryflux_core::security_setting::is_blank_security_setting(&v) {
                let (auth_cfg, authz_cfg) =
                    queryflux_core::security_setting::parse_security_setting(&v);
                if let Some(auth_cfg) = auth_cfg {
                    config.auth = auth_cfg;
                }
                if let Some(authz_cfg) = authz_cfg {
                    config.authorization = authz_cfg;
                }
            }
        }
        let mut routing_from_db = false;
        match pg.load_routing_config().await {
            Ok(Some(loaded)) => {
                config.routing_fallback = loaded.routing_fallback;
                let mut routers = Vec::new();
                for v in loaded.routers {
                    match serde_json::from_value::<queryflux_core::config::RouterConfig>(v) {
                        Ok(r) => routers.push(r),
                        Err(e) => {
                            tracing::warn!(error = %e, "Skipping invalid routing_rules row from Postgres")
                        }
                    }
                }
                config.routers = routers;
                routing_from_db = true;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, "load_routing_config failed; keeping YAML routing")
            }
        }
        if !routing_from_db {
            if let Ok(Some(v)) = pg.get_proxy_setting("routing_config").await {
                if let Ok(fallback) = serde_json::from_value::<String>(
                    v.get("routingFallback")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                ) {
                    config.routing_fallback = fallback;
                }
                if let Ok(routers) =
                    serde_json::from_value::<Vec<queryflux_core::config::RouterConfig>>(
                        v.get("routers").cloned().unwrap_or(serde_json::Value::Null),
                    )
                {
                    config.routers = routers;
                }
            }
        }
    }

    // Build the engine registry up front so it can be used for validation and AppState.
    let engine_registry = Arc::new(queryflux_core::engine_registry::EngineRegistry::new(
        registered_engines::all_descriptors(),
    ));

    // --- Validate cluster configs against the engine registry ---
    {
        use queryflux_core::engine_registry::validate_cluster_config;
        let mut all_errors: Vec<String> = Vec::new();
        for (name, cfg) in &config.clusters {
            all_errors.extend(validate_cluster_config(&engine_registry, name, cfg));
        }
        if !all_errors.is_empty() {
            for e in &all_errors {
                tracing::error!("{e}");
            }
            anyhow::bail!(
                "Config validation failed with {} error(s)",
                all_errors.len()
            );
        }
    }

    // --- Build translation service ---
    let translation = Arc::new(
        TranslationService::new_sqlglot(config.translation.python_scripts.clone())
            .unwrap_or_else(|e| {
                tracing::warn!(mode = ?config.translation.mode, "sqlglot unavailable ({e}); required translation will follow the configured policy");
                TranslationService::unavailable(config.translation.python_scripts.clone())
            })
            .with_policy(
                config.translation.mode,
                config.translation.error_on_unsupported,
            )
            .with_schema_resolution_timeout(std::time::Duration::from_millis(
                config.translation.schema_resolution_timeout_ms,
            )),
    );

    // --- Build catalog provider ---
    // Feeds `translation.resolve_schema_context` so sqlglot's optimizer can qualify
    // columns/types instead of falling back to dialect-only translation. Never fails
    // startup: an unimplemented or misconfigured integration degrades to a no-op
    // (see `queryflux_catalog::build_catalog_provider`), same philosophy as `translation` above.
    let catalog: Arc<dyn queryflux_core::catalog::CatalogProvider> =
        queryflux_catalog::build_catalog_provider(&config.catalog_provider).await;

    config
        .validate_startup_security()
        .map_err(|e| anyhow::anyhow!("Startup security validation failed: {e}"))?;

    let auth_provider = build_auth_provider(&config.auth)?;
    let authorization = build_authorization(
        &config.authorization,
        &config.cluster_groups,
        operators_from_auth(&config.auth),
    )?;

    if !is_worker {
        // --- Production safety warnings ---
        if matches!(
            config.auth.provider,
            queryflux_core::config::AuthProviderConfig::None
        ) {
            tracing::warn!(
                "SECURITY: auth.provider is 'none' — all query frontends accept unauthenticated traffic. \
                 Set auth.provider to 'oidc', 'ldap', or 'static' and auth.required = true for production."
            );
        }
        if !config.auth.required {
            tracing::warn!(
                "SECURITY: auth.required is false — unauthenticated requests are allowed even when \
                 an auth provider is configured. Set auth.required = true for production."
            );
        }
        {
            let effective_user = std::env::var("QUERYFLUX_ADMIN_USER")
                .unwrap_or_else(|_| config.queryflux.admin_api.username.clone());
            let effective_pass = std::env::var("QUERYFLUX_ADMIN_PASSWORD")
                .unwrap_or_else(|_| config.queryflux.admin_api.password.clone());
            if effective_user == "admin" && effective_pass == "admin" {
                tracing::warn!(
                    "SECURITY: admin API is using default credentials (admin/admin). \
                     Change via QUERYFLUX_ADMIN_USER / QUERYFLUX_ADMIN_PASSWORD or the Studio UI."
                );
            }
        }

        // --- Startup validation: engine × queryAuth support matrix ---
        // Centralized in `query_auth_supported` so YAML load, Studio PUT, and this check can't
        // drift apart. `passthrough`/`impersonate` are Trino-only; `tokenExchange` is Trino or
        // ADBC-with-an-OAuth-capable-driver (`cfg.driver`, only ever set for admin-API-created
        // ADBC clusters — see `ClusterConfig::driver`).
        for (name, cfg) in &config.clusters {
            if let Some(mode) = &cfg.query_auth {
                if let Err(msg) = queryflux_core::config::query_auth_supported(
                    cfg.engine.as_ref(),
                    cfg.driver.as_deref(),
                    mode,
                ) {
                    anyhow::bail!("cluster '{name}': {msg}");
                }
            }
        }

        // Deprecation warning: a cluster with no explicit `queryAuth` (or `serviceAccount`) and
        // no HTTP-setting cluster auth still implicitly forwards the client's own Authorization
        // header today, for backward compatibility. Operators should migrate to an explicit
        // `queryAuth: passthrough` — this fallback may be removed in a future release.
        for (name, cfg) in &config.clusters {
            let is_implicit_passthrough_eligible = matches!(
                cfg.query_auth,
                None | Some(queryflux_core::config::QueryAuthConfig::ServiceAccount)
            ) && matches!(
                cfg.engine,
                Some(queryflux_core::config::EngineConfig::Trino)
            ) && !cfg
                .auth
                .as_ref()
                .is_some_and(|a| a.sets_http_authorization());
            if is_implicit_passthrough_eligible {
                tracing::warn!(
                    "DEPRECATED: cluster '{name}' has no HTTP cluster auth and no explicit \
                     queryAuth — it still implicitly forwards the client's Authorization header \
                     (legacy behavior). Set queryAuth.type: passthrough explicitly; the implicit \
                     fallback may be removed in a future release."
                );
            }
        }

        // Startup warning: with no gateway-level authorization policy, `passthrough` forwards
        // whatever the client sent straight to the backend. The gap exists regardless of how
        // many cluster groups there are — a single-group deployment is the common default, so
        // gating on `cluster_groups.len() > 1` hid the warning for exactly the most likely
        // misconfiguration. Group count only changes the blast radius, not whether the gap
        // exists, so it's now informational in the message rather than a gate.
        let passthrough_clusters = unauthenticated_passthrough_clusters(&config);
        if !passthrough_clusters.is_empty() {
            tracing::warn!(
                clusters = ?passthrough_clusters,
                "SECURITY: authorization.provider is 'none' and queryAuth: passthrough is set \
                 on {} cluster(s) across {} cluster group(s) — QueryFlux is not enforcing any \
                 access policy, so a client's own credential decides what it can reach on \
                 every group it can route to. Configure authorization or narrow routing.",
                passthrough_clusters.len(),
                config.cluster_groups.len()
            );
        }
    }

    let identity_resolver = Arc::new(BackendIdentityResolver::new());

    let group_translation_scripts: HashMap<String, Vec<String>> = if let Some(pg) = &backend {
        let loaded = pg
            .load_group_translation_bodies()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load group translation scripts from Postgres: {e}");
                HashMap::new()
            });
        validate_group_translation_scripts(loaded)
    } else {
        HashMap::new()
    };
    let guard_script_bodies =
        load_guard_script_bodies(backend.as_deref().map(|b| b as &dyn AdminStore)).await;

    // --- Build the data access-control guard (OPA row filtering / column masking /
    // table-column allow-deny), if configured. Runs on the source SQL, before
    // dialect translation — see `queryflux_frontend::access_control_guard`.
    let access_control_guard = build_access_control_guard(&config, backend.as_deref()).await?;

    // --- Build guard chains: DB-stored config (UI-managed) takes precedence over YAML ---
    // When a persisted config exists in Postgres it is authoritative, even if it
    // resolves to an empty chain (the user may have intentionally cleared guards).
    let (guard_chain, group_guard_chains) = if let Some(pg) = &backend {
        match pg.get_proxy_setting("guardrails_config").await {
            Ok(Some(v)) => build_guard_chains_from_db_value(&v, &guard_script_bodies),
            _ => build_guard_chains(&config, &guard_script_bodies),
        }
    } else {
        build_guard_chains(&config, &guard_script_bodies)
    };

    // --- Compile the config into a LiveConfig (adapters, groups, routing, validation) ---
    // Same compiler as hot reload, in strict mode: an unknown group member, a strategy
    // that fails to build, an unreadable routing script or a dangling reference aborts
    // startup.
    let inputs = LiveConfigInputs {
        clusters: startup_cluster_specs.unwrap_or_else(|| {
            config
                .clusters
                .iter()
                .map(|(name, cfg)| ClusterSpec::from_yaml(name, cfg))
                .collect()
        }),
        groups: config.cluster_groups.clone(),
        routers: config.routers.clone(),
        routing_fallback: config.routing_fallback.clone(),
        group_translation_scripts,
        // Startup applies settings itself (YAML with DB overrides, below and above).
        settings: Default::default(),
    };
    let ids = ConfigIds {
        clusters: cluster_ids_by_name,
        groups: group_ids_by_name,
    };
    let mut reload_cache = AdapterReloadCache {
        adapters: HashMap::new(),
        config_json: HashMap::new(),
        cluster_states: HashMap::new(),
        routing_fallback: config.routing_fallback.clone(),
        routers_cfg: config.routers.clone(),
        strategies: HashMap::new(),
    };
    let mut live_config = if is_worker {
        crate::live_config::empty_live_config()
    } else {
        compile_live_config(&inputs, &ids, &mut reload_cache, CompileMode::Strict)
            .await
            .context("Startup config is invalid — aborting")?
    };
    live_config.guard_chain = guard_chain;
    live_config.group_guard_chains = group_guard_chains;
    live_config.access_control_guard = access_control_guard;
    live_config.auth_provider = auth_provider;
    live_config.authorization = authorization;
    live_config.catalog = catalog;
    let adapter_reload_cache = Arc::new(tokio::sync::Mutex::new(reload_cache));
    let live = Arc::new(tokio::sync::RwLock::new(live_config));
    let worker_sync_state = Arc::new(std::sync::Mutex::new(
        crate::config_source::WorkerSyncState::default(),
    ));
    // A worker keeps no durable history: its records go upstream, to the control plane
    // or the endpoint its snapshot names.
    let history_uplink = match (&config.queryflux.control_plane, is_worker) {
        (Some(cp), true) => {
            let history = config.queryflux.history.clone().unwrap_or_default();
            Some(history_uplink::RemoteMetricsStore::new(
                cp,
                startup::tasks::worker_name(&config),
                history.endpoint,
                history.spool_max_records,
                worker_sync_state.clone(),
            ))
        }
        _ => None,
    };
    let metrics: Arc<dyn queryflux_metrics::MetricsStore> = match &history_uplink {
        Some(uplink) => Arc::new(queryflux_metrics::MultiMetricsStore::new(vec![
            metrics,
            uplink.clone() as Arc<dyn queryflux_metrics::MetricsStore>,
        ])),
        None => metrics,
    };

    // Replica identity for capacity leases and queue claims. Must be unique per
    // *process incarnation*: PIDs collide across containers (the main process is
    // PID 1 in most pods), and a bare hostname survives container restarts —
    // either would make this replica's heartbeat renew leases that belong to a
    // dead instance, so they would never expire. Hostname (= pod name in
    // Kubernetes) is included purely for debuggability; the random nonce is what
    // guarantees uniqueness.
    let instance_id = std::env::var("QUERYFLUX_INSTANCE_ID").unwrap_or_else(|_| {
        let host =
            std::env::var("HOSTNAME").unwrap_or_else(|_| format!("pid{}", std::process::id()));
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        format!("qf-{host}-{}", &nonce[..8])
    });
    tracing::info!(instance_id = %instance_id, "Replica instance ID");

    // Distributed mode detection and validation. Resolved before AppState is
    // built so the flag actually gates coordination: with `distributed: false`
    // no capacity leases are taken, no queue claims are made, and the
    // heartbeat/expiry/reconcile tasks (all keyed on `capacity_store`) stay off.
    let distributed = config
        .queryflux
        .resolve_distributed(
            coordination
                .as_ref()
                .is_some_and(|b| b.supports_distributed_coordination()),
        )
        .map_err(|e| anyhow::anyhow!(e))?;

    if distributed && !is_worker {
        tracing::warn!(
            "Distributed coordination is enabled. \
             This requires HA Postgres — during a Postgres outage new queries cannot take \
             a capacity lease and wait in the queue (fail-closed) until it returns. \
             Alert on queryflux_coordination_failures_total > 0."
        );
    }

    let capacity_store: Option<Arc<dyn queryflux_persistence::CapacityStore>> = distributed
        .then(|| {
            coordination
                .clone()
                .map(|b| b as Arc<dyn queryflux_persistence::CapacityStore>)
        })
        .flatten();
    let queue_coordinator: Option<Arc<dyn queryflux_persistence::QueueCoordinator>> = distributed
        .then(|| {
            distributed_backend
                .clone()
                .map(|b| b as Arc<dyn queryflux_persistence::QueueCoordinator>)
        })
        .flatten();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Initialize the query result cache (startup-only, not hot-reloadable).
    let result_cache: Arc<dyn queryflux_cache::QueryResultCache> = match &config
        .queryflux
        .cache_backend
    {
        Some(cache_cfg) => {
            let cache_store: Arc<dyn queryflux_persistence::CacheStore> =
                if let Some(b) = backend.clone() {
                    b as Arc<dyn queryflux_persistence::CacheStore>
                } else {
                    mem_store
                        .clone()
                        .expect("mem_store must exist when no backend")
                        as Arc<dyn queryflux_persistence::CacheStore>
                };
            match queryflux_cache::opendal_cache::OpenDalResultCache::new(cache_cfg, cache_store) {
                Ok(c) => {
                    tracing::info!("Query result cache enabled (scheme={})", cache_cfg.scheme);
                    Arc::new(c)
                }
                Err(e) => {
                    tracing::error!("Failed to initialize result cache: {e}; caching disabled");
                    Arc::new(queryflux_cache::noop::NoopResultCache)
                }
            }
        }
        None => Arc::new(queryflux_cache::noop::NoopResultCache),
    };

    let app_state = Arc::new(AppState {
        external_address: external_address.clone(),
        live: live.clone(),
        persistence,
        translation,
        metrics,
        identity_resolver,
        capacity_store,
        queue_coordinator,
        instance_id,
        http_client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("build shared http client"),
        result_cache,
    });

    // --- Start admin server (Prometheus /metrics + future /admin/* endpoints) ---
    let admin_port = config.queryflux.admin_api.port;
    let admin_store: Option<Arc<dyn AdminStore>> = backend
        .clone()
        .map(|b| b as Arc<dyn AdminStore>)
        .or_else(|| mem_store.clone().map(|m| m as Arc<dyn AdminStore>));
    let security_config = Arc::new(AdminSecurityConfigDto::from_config(
        &config.auth,
        &config.authorization,
        &config.cluster_groups,
    ));
    let routing_config = Arc::new(AdminRoutingConfigDto::from_config(
        &config.routing_fallback,
        &config.routers,
    ));
    let catalog_provider_config = Arc::new(config.catalog_provider.clone());
    let access_control_config = Arc::new(config.access_control.clone());
    let config_reload_notify = Arc::new(tokio::sync::Notify::new());

    let frontends_status = build_frontends_status(
        &config.queryflux.frontends,
        admin_port,
        config.queryflux.external_address.clone(),
    );

    // Build admin credentials — env vars take precedence over YAML.
    // Bootstrap lives on `queryflux.adminApi`, not the unused top-level `adminApi`.
    let admin_username = std::env::var("QUERYFLUX_ADMIN_USER")
        .unwrap_or_else(|_| config.queryflux.admin_api.username.clone());
    let admin_password = std::env::var("QUERYFLUX_ADMIN_PASSWORD")
        .unwrap_or_else(|_| config.queryflux.admin_api.password.clone());
    let settings_store = backend
        .clone()
        .map(|b| b as Arc<dyn queryflux_persistence::ProxySettingsStore>)
        .or_else(|| {
            mem_store
                .clone()
                .map(|m| m as Arc<dyn queryflux_persistence::ProxySettingsStore>)
        });
    let admin_creds = Arc::new(queryflux_auth::AdminCredentialsManager::new(
        admin_username,
        admin_password,
        settings_store,
        backend.is_some(),
    ));

    let test_cluster_fn: TestClusterFn = Arc::new(|engine_key, config_json| {
        Box::pin(async move {
            let adapter = registered_engines::build_adapter_from_record(
                ClusterName("__test__".to_string()),
                ClusterGroupName("__test__".to_string()),
                &engine_key,
                &config_json,
            )
            .await?;
            Ok(adapter.health_check().await)
        })
    });

    let test_catalog_provider_fn: TestCatalogProviderFn = Arc::new(|config_json| {
        Box::pin(async move {
            let cfg = serde_json::from_value::<queryflux_core::config::CatalogProviderConfig>(
                config_json,
            )?;
            if matches!(cfg, queryflux_core::config::CatalogProviderConfig::Null) {
                return Ok((true, "No catalog configured (type: null).".to_string()));
            }
            // Uses the fallible builder, not `build_catalog_provider` — this
            // needs the real construction error, not a silent degrade to a
            // no-op. Every real provider ignores its `catalog` argument (Glue/
            // HiveMetastore/IcebergRest each expose exactly one synthetic
            // catalog), so any string works here.
            let provider = match queryflux_catalog::try_build_catalog_provider(&cfg).await {
                Ok(provider) => provider,
                Err(e) => return Ok((false, format!("Failed to build provider: {e}"))),
            };
            if matches!(
                cfg,
                queryflux_core::config::CatalogProviderConfig::Static { .. }
            ) {
                return match provider.list_tables("", "").await {
                    Ok(tables) => Ok((
                        true,
                        format!("Static catalog — {} table(s) configured", tables.len()),
                    )),
                    Err(e) => Ok((
                        false,
                        format!("Built static catalog, but listing tables failed: {e}"),
                    )),
                };
            }
            // `list_catalogs()` is a hardcoded synthetic single-entry result for
            // every real provider (Glue/HMS/Iceberg REST have no native "list
            // catalogs" call) — it makes no network call at all, so it can't
            // prove connectivity. `list_databases` does: it's the cheapest call
            // that actually round-trips to the backing service for all three.
            match provider.list_databases("").await {
                Ok(databases) => Ok((
                    true,
                    format!("Connected — {} database(s) visible", databases.len()),
                )),
                Err(e) => Ok((
                    false,
                    format!("Built provider, but listing databases failed: {e}"),
                )),
            }
        })
    });

    let admin_store_for_reload = admin_store.clone();
    let cors_origins = config.queryflux.admin_api.cors_allowed_origins.clone();
    if cors_origins.is_empty() {
        tracing::warn!(
            "Admin API CORS allows any origin (corsAllowedOrigins is empty). \
             Set queryflux.adminApi.corsAllowedOrigins to restrict cross-origin access in production."
        );
    }
    // Bumped after every config reload; wakes workers' long-polls.
    let (config_changed_tx, config_changed_rx) = tokio::sync::watch::channel(0u64);
    let worker_api = startup::worker_api::worker_api(
        &config,
        snapshot_store,
        &inputs,
        worker_registry,
        config_changed_rx,
        served_coordination.filter(|_| distributed),
        history_store,
    )?;
    let admin = AdminFrontend::new(
        prometheus.clone(),
        live.clone(),
        admin_store,
        admin_port,
        security_config,
        routing_config,
        catalog_provider_config,
        access_control_config,
        engine_registry,
        config_reload_notify.clone(),
        frontends_status,
        admin_creds,
        test_cluster_fn,
        test_catalog_provider_fn,
        cors_origins,
        app_state.result_cache.clone(),
        app_state.clone(),
    );
    let admin = match worker_api {
        Some(api) => admin.with_worker_api(api),
        None => admin,
    };

    // --- Start Trino HTTP frontend (honors frontends.trinoHttp.enabled) ---
    let trino_cfg = config.queryflux.frontends.trino_http.clone();
    let trino_port = trino_cfg.port;

    if trino_cfg.enabled {
        info!(
            "QueryFlux ready — Trino HTTP on :{trino_port}, admin/metrics on :{admin_port}, external address: {external_address}"
        );
    } else {
        info!(
            "QueryFlux ready — Trino HTTP disabled, admin/metrics on :{admin_port}, external address: {external_address}"
        );
    }

    if distributed && !is_worker {
        if config
            .queryflux
            .periodic_config_reload_interval_secs()
            .is_none()
        {
            tracing::warn!(
                "Distributed mode with configReloadIntervalSecs: 0 — periodic config polling \
                 is disabled. Config propagation relies solely on LISTEN/NOTIFY; if the \
                 notification channel drops, replicas may become stale."
            );
        }
        tracing::info!(
            instance_id = %app_state.instance_id,
            "Distributed mode enabled — global capacity, config revision, and queue \
             coordination are active via the persistence backend"
        );
    }

    if backend.is_some() {
        match config.queryflux.periodic_config_reload_interval_secs() {
            None => tracing::info!(
                "Postgres persistence: routing rules and cluster/group config are cached in memory; periodic DB refresh is disabled (configReloadIntervalSecs: 0). Reloads still run after Studio/admin API writes."
            ),
            Some(secs) => tracing::info!(
                secs,
                "Postgres persistence: routing rules and cluster/group config are cached in memory and reloaded from the DB on this interval (seconds), or immediately after Studio/admin writes"
            ),
        }
    }

    startup::tasks::spawn_cluster_snapshots(
        &app_state,
        &prometheus,
        &backend,
        &local_sweeps,
        &shutdown_rx,
    );

    startup::tasks::spawn_lease_heartbeat(&app_state, &shutdown_rx);

    startup::tasks::spawn_zombie_sweep(&app_state, &local_sweeps, &shutdown_rx);

    startup::tasks::spawn_stale_queue_sweep(&app_state, &local_sweeps, &shutdown_rx);

    startup::tasks::spawn_cache_cleanup(&app_state, &shutdown_rx, &config);

    startup::tasks::spawn_history_retention(&backend, &shutdown_rx, &config);

    // Background task: hot-reload config. The source decides when (admin writes on this
    // replica, revision changes from other replicas, a periodic safety-net timer) and how
    // (Postgres rows, or in YAML mode only the settings stored through the admin API).
    {
        let revisions = match &backend {
            Some(backend) => match backend.subscribe_revisions().await {
                Ok(Some(rx)) => {
                    tracing::info!("Subscribed to backend config revision notifications");
                    Some(rx)
                }
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!("Failed to subscribe to config revision notifications: {e}");
                    None
                }
            },
            None => None,
        };
        let triggers = ReloadTriggers::new(
            config_reload_notify.clone(),
            revisions,
            config.queryflux.periodic_config_reload_interval_secs(),
        )
        .await;
        let source: Box<dyn ConfigSource> = match (&config.queryflux.control_plane, &backend) {
            (Some(cp), _) if is_worker => Box::new(crate::config_source::RemoteConfigSource::new(
                cp,
                worker_sync_state.clone(),
            )),
            (_, Some(backend)) => Box::new(PostgresConfigSource::new(backend.clone(), triggers)),
            (_, None) => Box::new(AdminStoreConfigSource::new(
                admin_store_for_reload,
                triggers,
            )),
        };
        let handles = LiveHandles {
            live: live.clone(),
            cache: adapter_reload_cache.clone(),
            metrics: app_state.metrics.clone(),
            changed: Some(config_changed_tx),
        };
        tokio::spawn(run_reload_loop(source, handles, shutdown_rx.clone()));
        if is_worker {
            startup::tasks::spawn_worker_heartbeat(&config, &worker_sync_state, &shutdown_rx);
            if let Some(uplink) = &history_uplink {
                uplink.spawn_flusher(shutdown_rx.clone());
            }
        }
    }

    startup::tasks::spawn_health_checks(&app_state, &shutdown_rx);

    startup::tasks::spawn_engine_reconcile(&app_state, &coordination, &shutdown_rx);

    // Spawn all enabled frontends as tasks. Each frontend observes `shutdown_rx`
    // internally: axum-based frontends use `with_graceful_shutdown` (stop accepting,
    // finish in-flight requests), wire-based frontends break their accept loop, and
    // tonic (Flight SQL) uses `serve_with_shutdown`.
    let mut frontend_tasks = startup::frontends::FrontendTasks::default();

    if is_worker {
        let ops = queryflux_frontend::ops::OpsFrontend::new(
            prometheus.clone(),
            live.clone(),
            config.queryflux.admin_api.port,
            {
                let state = worker_sync_state.clone();
                Arc::new(move || {
                    let st = state.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    serde_json::to_value(st).unwrap_or_default()
                })
            },
        );
        frontend_tasks.spawn("Ops", {
            let rx = shutdown_rx.clone();
            async move { ops.listen(rx).await }
        });
    } else {
        frontend_tasks.spawn("Admin", {
            let rx = shutdown_rx.clone();
            async move { admin.listen(rx).await }
        });
    }
    startup::frontends::spawn_query_frontends(
        &mut frontend_tasks,
        &app_state,
        &config,
        trino_cfg,
        &shutdown_rx,
    );

    startup::shutdown::wait_for_shutdown(&mut frontend_tasks).await;
    startup::shutdown::graceful_shutdown(
        &shutdown_tx,
        &mut frontend_tasks,
        &app_state,
        config.queryflux.shutdown_drain_timeout_secs(),
    )
    .await;
    if let Some(uplink) = &history_uplink {
        uplink.final_flush(std::time::Duration::from_secs(10)).await;
    }

    tracing::info!("QueryFlux shutdown complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Hot-reload helpers
// ---------------------------------------------------------------------------

/// Clusters with `queryAuth: passthrough` when `authorization.provider` is `none` — the
/// gateway enforces no access policy, so a client's own credential decides what it can
/// reach. Returned regardless of `cluster_groups.len()`: a single-group deployment (the
/// common default) has this gap just as much as a multi-group one, it's just narrower in
/// blast radius, which is why the caller reports the group count rather than gating on it.
fn unauthenticated_passthrough_clusters(
    config: &queryflux_core::config::ProxyConfig,
) -> Vec<&String> {
    if !matches!(
        config.authorization.provider,
        queryflux_core::config::AuthorizationProviderConfig::None
    ) {
        return Vec::new();
    }
    // `build_authorization` still enforces a per-group SimpleAuthorizationPolicy when
    // provider is `none` but any group declares an allow-list — mirror that check so this
    // warning only fires on the genuinely allow-all case, not one that already has policy.
    let has_any_policy = config.cluster_groups.values().any(|cfg| {
        !cfg.authorization.allow_groups.is_empty() || !cfg.authorization.allow_users.is_empty()
    });
    if has_any_policy {
        return Vec::new();
    }
    config
        .clusters
        .iter()
        .filter(|(_, cfg)| {
            matches!(
                cfg.query_auth,
                Some(queryflux_core::config::QueryAuthConfig::Passthrough)
            )
        })
        .map(|(name, _)| name)
        .collect()
}

#[cfg(test)]
mod tests {
    mod circuit_breaker_reload {
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::time::Duration;

        use queryflux_cluster_manager::circuit_breaker::{BackendOutcome, CircuitState};
        use queryflux_cluster_manager::cluster_state::ClusterState;
        use queryflux_core::config::{CircuitBreakerConfig, ClusterGroupConfig};
        use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};

        use super::super::AdapterReloadCache;
        use crate::live_config::{compile_live_config, CompileMode, ConfigIds, LiveConfigInputs};
        use crate::startup::tasks::{reconcile_targets_from_health_targets, BreakerProbeGuard};

        fn state(group: &str, name: &str, breaker: bool) -> Arc<ClusterState> {
            let state = ClusterState::new(
                ClusterName(name.to_string()),
                ClusterGroupName(group.to_string()),
                None,
                None,
                EngineType::ClickHouse,
                None,
                10,
                true,
            );
            Arc::new(if breaker {
                state.with_circuit_breaker(
                    Some(CircuitBreakerConfig {
                        min_requests: 1,
                        failure_rate_percent: 100,
                        initial_backoff_secs: 1,
                        max_backoff_secs: 2,
                        ..CircuitBreakerConfig::default()
                    }),
                    None,
                )
            } else {
                state
            })
        }

        #[test]
        fn shared_backend_is_reconciled_once_but_updates_each_group() {
            let first = state("group-a", "shared", false);
            let second = state("group-b", "shared", false);
            let other = state("group-a", "other", false);
            let targets = vec![
                ("adapter-a", first.clone()),
                ("adapter-a", second.clone()),
                ("adapter-b", other.clone()),
            ];
            let grouped = reconcile_targets_from_health_targets(&targets);
            assert_eq!(grouped.len(), 2);
            assert_eq!(grouped[0].1.len(), 2);
            assert_eq!(grouped[1].1.len(), 1);
            for member in &grouped[0].1 {
                member.set_running_queries(3);
            }
            assert_eq!(first.running_queries(), 3);
            assert_eq!(second.running_queries(), 3);
            assert_eq!(other.running_queries(), 0);
        }

        #[tokio::test]
        async fn invalid_reload_does_not_replace_cached_breaker_states() {
            let old = state("group", "old", true);
            let key = ("group".to_string(), "old".to_string());
            let mut cache = AdapterReloadCache {
                adapters: HashMap::new(),
                config_json: HashMap::new(),
                cluster_states: HashMap::from([(key.clone(), old.clone())]),
                routing_fallback: "group".to_string(),
                routers_cfg: Vec::new(),
                strategies: HashMap::new(),
            };
            let group: ClusterGroupConfig = serde_json::from_value(serde_json::json!({
                "members": ["missing"],
                "maxRunningQueries": 10
            }))
            .expect("valid group fixture");
            let inputs = LiveConfigInputs {
                groups: HashMap::from([("group".to_string(), group)]),
                routing_fallback: "group".to_string(),
                ..LiveConfigInputs::default()
            };
            let result = compile_live_config(
                &inputs,
                &ConfigIds::default(),
                &mut cache,
                CompileMode::Lenient,
            )
            .await;
            assert!(result.is_err(), "unknown member must reject reload");
            assert!(Arc::ptr_eq(
                cache.cluster_states.get(&key).expect("old state retained"),
                &old
            ));
        }

        fn duck_record(
            config: serde_json::Value,
        ) -> queryflux_persistence::cluster_config::ClusterConfigRecord {
            queryflux_persistence::cluster_config::ClusterConfigRecord {
                id: 1,
                name: "duck".to_string(),
                engine_key: "duckDb".to_string(),
                enabled: true,
                max_running_queries: None,
                config,
                variants: serde_json::json!([]),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            }
        }

        async fn reload(
            record: &queryflux_persistence::cluster_config::ClusterConfigRecord,
            cache: &mut AdapterReloadCache,
        ) -> queryflux_frontend::state::LiveConfig {
            let group: ClusterGroupConfig = serde_json::from_value(serde_json::json!({
                "members": ["duck"],
                "maxRunningQueries": 10
            }))
            .expect("valid group fixture");
            let inputs = LiveConfigInputs::from_records(
                std::slice::from_ref(record),
                HashMap::from([("g".to_string(), group)]),
                Vec::new(),
                "g".to_string(),
                HashMap::new(),
            );
            compile_live_config(&inputs, &ConfigIds::default(), cache, CompileMode::Lenient)
                .await
                .expect("reload succeeds")
        }

        async fn running(live: &queryflux_frontend::state::LiveConfig) -> u64 {
            live.cluster_manager.all_cluster_states().await.unwrap()[0].running_queries
        }

        fn empty_cache() -> AdapterReloadCache {
            AdapterReloadCache {
                adapters: HashMap::new(),
                config_json: HashMap::new(),
                cluster_states: HashMap::new(),
                routing_fallback: "g".to_string(),
                routers_cfg: Vec::new(),
                strategies: HashMap::new(),
            }
        }

        /// Regression for #299: a query in flight across a reload releases through the
        /// generation that admitted it; the new generation must not keep its slot.
        #[tokio::test]
        async fn reload_does_not_leak_in_flight_running_count() {
            let record = duck_record(serde_json::json!({}));
            let mut cache = empty_cache();
            let group = ClusterGroupName("g".to_string());

            let gen1 = reload(&record, &mut cache).await;
            let cluster = gen1
                .cluster_manager
                .acquire_cluster(&group)
                .await
                .unwrap()
                .unwrap();
            let gen2 = reload(&record, &mut cache).await;
            assert_eq!(
                running(&gen2).await,
                1,
                "in-flight query still counts after reload"
            );

            gen1.cluster_manager
                .release_cluster(&group, &cluster)
                .await
                .unwrap();
            assert_eq!(running(&gen2).await, 0);
            let gen3 = reload(&record, &mut cache).await;
            assert_eq!(running(&gen3).await, 0);
        }

        /// A reconcile pass that captured the previous generation must not overwrite an
        /// admission made through the new generation while the engine fetch was in flight.
        #[tokio::test]
        async fn stale_generation_reconcile_keeps_new_generation_admissions() {
            let record = duck_record(serde_json::json!({}));
            let mut cache = empty_cache();
            let group = ClusterGroupName("g".to_string());

            let gen1 = reload(&record, &mut cache).await;
            // One leaked slot: the counter says 1, the engine runs nothing.
            gen1.cluster_manager
                .acquire_cluster(&group)
                .await
                .unwrap()
                .unwrap();
            let gen1_state = cache
                .cluster_states
                .get(&("g".to_string(), "duck".to_string()))
                .cloned()
                .expect("gen1 state cached");
            let tracked = gen1_state.running_queries(); // reconcile observes 1, starts the fetch

            // A reload shares the counter; the new generation admits a real query.
            let gen2 = reload(&record, &mut cache).await;
            gen2.cluster_manager
                .acquire_cluster(&group)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(running(&gen2).await, 2);

            // The engine count (0, taken before the admission) lands through the stale state:
            // only the leaked slot is removed, the new admission survives.
            crate::startup::tasks::apply_reconcile_to_cluster_state(&gen1_state, tracked, Some(0));
            assert_eq!(running(&gen2).await, 1);
        }

        /// A query admitted before a config-changing reload still counts against the member,
        /// and is released correctly through either generation (#301).
        #[tokio::test]
        async fn changed_config_reload_keeps_counting_in_flight_queries() {
            let mut cache = empty_cache();
            let group = ClusterGroupName("g".to_string());

            let gen1 = reload(&duck_record(serde_json::json!({})), &mut cache).await;
            let sync_slot = gen1
                .cluster_manager
                .acquire_cluster(&group)
                .await
                .unwrap()
                .unwrap();
            let async_slot = gen1
                .cluster_manager
                .acquire_cluster(&group)
                .await
                .unwrap()
                .unwrap();
            let gen2 = reload(&duck_record(serde_json::json!({"poolSize": 2})), &mut cache).await;
            assert_eq!(
                running(&gen2).await,
                2,
                "in-flight queries still occupy the member"
            );

            // Sync dispatch releases through the manager that admitted the query.
            gen1.cluster_manager
                .release_cluster(&group, &sync_slot)
                .await
                .unwrap();
            assert_eq!(running(&gen2).await, 1);
            // Async terminals (poll, cancel) release through the current manager.
            gen2.cluster_manager
                .release_cluster(&group, &async_slot)
                .await
                .unwrap();
            assert_eq!(running(&gen2).await, 0);
            assert_eq!(running(&gen1).await, 0);
        }

        #[test]
        fn dropped_probe_reopens_breaker_instead_of_stranding_half_open() {
            let member = state("group", "backend", true);
            assert_eq!(
                member.record_backend_outcome(BackendOutcome::Failure),
                Some(CircuitState::Open)
            );
            std::thread::sleep(Duration::from_secs(1));
            let probe = BreakerProbeGuard::reserve(&member).expect("probe should be ready");
            assert_eq!(member.breaker_state(), Some(CircuitState::HalfOpen));
            drop(probe);
            assert_eq!(member.breaker_state(), Some(CircuitState::Open));
        }

        #[test]
        fn completed_probe_is_not_reopened_on_drop() {
            let member = state("group", "backend", true);
            member.record_backend_outcome(BackendOutcome::Failure);
            std::thread::sleep(Duration::from_secs(1));
            let probe = BreakerProbeGuard::reserve(&member).expect("probe should be ready");
            assert_eq!(probe.finish(true), Some(CircuitState::Closed));
            assert_eq!(member.breaker_state(), Some(CircuitState::Closed));
        }
    }

    mod group_translation_scripts {
        use std::collections::HashMap;

        use super::super::validate_group_translation_scripts;

        #[test]
        fn drops_only_the_group_whose_script_fails_validation() {
            let scripts = HashMap::from([
                (
                    "good-group".to_string(),
                    vec!["def transform(sql, src, dst):\n    return sql\n".to_string()],
                ),
                (
                    "legacy-group".to_string(),
                    vec!["def transform(ast, src, dst):\n    return None\n".to_string()],
                ),
            ]);
            let valid = validate_group_translation_scripts(scripts);
            assert_eq!(valid.get("good-group").map(Vec::len), Some(1));
            assert_eq!(valid.get("legacy-group").map(Vec::len), Some(0));
        }
    }

    mod frontend_tasks {
        use std::time::Duration;

        use crate::startup::frontends::FrontendTasks;

        #[tokio::test]
        async fn disabled_frontend_does_not_block_shutdown_drain() {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let mut frontends = FrontendTasks::default();
            let mut disabled_task_constructed = false;

            frontends.spawn("enabled", async move {
                shutdown_rx
                    .wait_for(|shutdown| *shutdown)
                    .await
                    .expect("shutdown sender should remain available");
                Ok(())
            });
            frontends.spawn_if_enabled("disabled", false, || {
                disabled_task_constructed = true;
                std::future::pending()
            });

            assert!(!disabled_task_constructed);

            shutdown_tx
                .send(true)
                .expect("enabled frontend is listening");
            tokio::time::timeout(Duration::from_secs(1), frontends.drain())
                .await
                .expect("disabled frontend must not hold the drain open");

            assert!(frontends.outstanding_names().is_empty());
        }
    }

    mod unauthenticated_passthrough_warning {
        use super::super::unauthenticated_passthrough_clusters;
        use queryflux_core::config::ProxyConfig;
        use serde_json::json;

        fn config(authorization_provider: &str, clusters: serde_json::Value) -> ProxyConfig {
            config_with_groups(authorization_provider, clusters, json!({}))
        }

        fn config_with_groups(
            authorization_provider: &str,
            clusters: serde_json::Value,
            cluster_groups: serde_json::Value,
        ) -> ProxyConfig {
            serde_json::from_value(json!({
                "queryflux": { "externalAddress": null },
                "clusters": clusters,
                "clusterGroups": cluster_groups,
                "authorization": { "provider": authorization_provider },
            }))
            .expect("valid minimal ProxyConfig fixture")
        }

        fn passthrough_cluster() -> serde_json::Value {
            json!({
                "trino-1": {
                    "engine": "trino",
                    "endpoint": "http://trino:8080",
                    "queryAuth": { "type": "passthrough" },
                }
            })
        }

        #[test]
        fn warns_even_with_a_single_cluster_group() {
            // Regression: this used to be gated on `cluster_groups.len() > 1`, hiding the
            // warning for the common single-group default — exactly the most likely
            // deployment to have this gap.
            let cfg = config("none", passthrough_cluster());
            assert_eq!(cfg.cluster_groups.len(), 0);
            let flagged = unauthenticated_passthrough_clusters(&cfg);
            assert_eq!(flagged, vec!["trino-1"]);
        }

        #[test]
        fn no_warning_when_authorization_is_configured() {
            let cfg = config("openfga", passthrough_cluster());
            assert!(unauthenticated_passthrough_clusters(&cfg).is_empty());
        }

        #[test]
        fn no_warning_without_any_passthrough_cluster() {
            let cfg = config(
                "none",
                json!({
                    "trino-1": {
                        "engine": "trino",
                        "endpoint": "http://trino:8080",
                    }
                }),
            );
            assert!(unauthenticated_passthrough_clusters(&cfg).is_empty());
        }

        #[test]
        fn no_warning_when_a_group_allow_list_already_enforces_policy() {
            // build_authorization treats provider:none + any group allow-list as an
            // enforced SimpleAuthorizationPolicy, not allow-all — this check must agree,
            // or the warning fires for deployments that already have the gap closed.
            let cfg = config_with_groups(
                "none",
                passthrough_cluster(),
                json!({
                    "trino": {
                        "members": ["trino-1"],
                        "maxRunningQueries": 10,
                        "authorization": { "allowGroups": ["data-team"] },
                    }
                }),
            );
            assert!(unauthenticated_passthrough_clusters(&cfg).is_empty());
        }

        #[test]
        fn warns_when_groups_exist_but_none_declare_an_allow_list() {
            let cfg = config_with_groups(
                "none",
                passthrough_cluster(),
                json!({
                    "trino": {
                        "members": ["trino-1"],
                        "maxRunningQueries": 10,
                    }
                }),
            );
            assert_eq!(unauthenticated_passthrough_clusters(&cfg), vec!["trino-1"]);
        }
    }

    mod guard_chains {
        use std::collections::{BTreeMap, HashMap};

        use queryflux_core::config::{GuardKindConfig, GuardSpecConfig, GuardrailsConfig};
        use queryflux_core::query::{ClusterGroupName, EngineType, SqlDialect};
        use queryflux_core::tags::QueryTags;
        use queryflux_guardrails::context::{GuardChainOutcome, GuardContext, GuardLayer};

        use crate::guards::{build_chain_from_db_specs, build_chain_from_yaml_specs};

        fn is_blocked(o: &GuardChainOutcome) -> bool {
            matches!(o, GuardChainOutcome::Blocked { .. })
        }

        static EMPTY_S: Vec<String> = Vec::new();
        static GENERIC_DIALECT: SqlDialect = SqlDialect::Generic;
        static EMPTY_ATTRS: std::sync::LazyLock<BTreeMap<String, serde_json::Value>> =
            std::sync::LazyLock::new(BTreeMap::new);
        static EMPTY_EXTRA: std::sync::LazyLock<HashMap<String, String>> =
            std::sync::LazyLock::new(HashMap::new);

        fn plan_ctx<'a>(
            engine: &'a EngineType,
            group: &'a ClusterGroupName,
            tags: &'a QueryTags,
        ) -> GuardContext<'a> {
            GuardContext {
                sql: "SELECT 1",
                original_sql: None,
                dialect: &GENERIC_DIALECT,
                engine_type: engine,
                cluster_group: group,
                user: Some("alice"),
                groups: &EMPTY_S,
                roles: &EMPTY_S,
                attributes: &EMPTY_ATTRS,
                agent_context: None,
                query_tags: tags,
                session_extra: &EMPTY_EXTRA,
                schema: None,
                sql_parse: None,
            }
        }

        #[tokio::test]
        async fn yaml_unknown_built_in_denies_at_runtime() {
            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = vec![GuardSpecConfig {
                kind: GuardKindConfig::BuiltIn,
                name: Some("does_not_exist".to_string()),
                script_id: None,
                script: None,
                url: None,
                timeout_ms: None,
                retry_count: None,
                fail_behavior: None,
                headers: None,
                max_rows: None,
                applies_to: None,
            }];
            let chain = build_chain_from_yaml_specs(&specs, &HashMap::new())
                .expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (actions, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(is_blocked(&outcome));
            assert_eq!(actions.len(), 1);
            assert_eq!(actions[0].guard, "built_in");
            assert_eq!(actions[0].action, "deny");
        }

        #[tokio::test]
        async fn yaml_python_script_without_body_denies_at_runtime() {
            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = vec![GuardSpecConfig {
                kind: GuardKindConfig::PythonScript,
                name: None,
                script_id: None,
                script: None,
                url: None,
                timeout_ms: None,
                retry_count: None,
                fail_behavior: None,
                headers: None,
                max_rows: None,
                applies_to: None,
            }];
            let err = GuardrailsConfig {
                global: specs.clone(),
                groups: HashMap::new(),
            }
            .validate()
            .expect_err("startup validation must reject");
            assert!(err.contains("script"), "{err}");

            // Misconfigured guards still deny if validation is bypassed (e.g. DB reload).
            let chain = build_chain_from_yaml_specs(&specs, &HashMap::new())
                .expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (_, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(is_blocked(&outcome));
        }

        #[tokio::test]
        async fn db_unknown_kind_denies_at_runtime() {
            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = serde_json::json!([{ "kind": "future_kind" }]);
            let chain =
                build_chain_from_db_specs(&specs, &HashMap::new()).expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (actions, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(is_blocked(&outcome));
            assert_eq!(actions[0].guard, "guard");
            assert!(actions[0]
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("future_kind"));
        }

        #[tokio::test]
        async fn inline_python_script_guard_allows() {
            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = vec![GuardSpecConfig {
                kind: GuardKindConfig::PythonScript,
                name: None,
                script_id: None,
                script: Some("def check(ctx):\n    return {'action': 'allow'}".to_string()),
                url: None,
                timeout_ms: Some(500),
                retry_count: None,
                fail_behavior: None,
                headers: None,
                max_rows: None,
                applies_to: None,
            }];
            let chain = build_chain_from_yaml_specs(&specs, &HashMap::new())
                .expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (_, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(!is_blocked(&outcome));
        }

        #[tokio::test]
        async fn non_http_webhook_url_denies_even_when_fail_open() {
            use queryflux_core::config::GuardFailBehaviorConfig;

            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = vec![GuardSpecConfig {
                kind: GuardKindConfig::HttpWebhook,
                name: None,
                script_id: None,
                script: None,
                url: Some("file:///tmp/policy".to_string()),
                timeout_ms: Some(100),
                retry_count: None,
                fail_behavior: Some(GuardFailBehaviorConfig::Allow),
                headers: None,
                max_rows: None,
                applies_to: None,
            }];
            let chain = build_chain_from_yaml_specs(&specs, &HashMap::new())
                .expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (actions, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(
                is_blocked(&outcome),
                "non-http(s) webhook URL must deny at construction"
            );
            assert_eq!(actions[0].guard, "http_webhook");
            assert!(actions[0]
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("http or https"));
        }

        #[tokio::test]
        async fn http_webhook_url_is_accepted_and_can_fail_open() {
            use queryflux_core::config::GuardFailBehaviorConfig;

            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = vec![GuardSpecConfig {
                kind: GuardKindConfig::HttpWebhook,
                name: None,
                script_id: None,
                script: None,
                url: Some("http://127.0.0.1:1/unreachable".to_string()),
                timeout_ms: Some(100),
                retry_count: Some(0),
                fail_behavior: Some(GuardFailBehaviorConfig::Allow),
                headers: None,
                max_rows: None,
                applies_to: None,
            }];
            let chain = build_chain_from_yaml_specs(&specs, &HashMap::new())
                .expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (_, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(
                !is_blocked(&outcome),
                "valid http(s) webhook with fail_open must allow when unreachable"
            );
        }

        #[tokio::test]
        async fn db_non_http_webhook_url_denies_at_runtime() {
            let engine = EngineType::DuckDb;
            let group = ClusterGroupName("default".to_string());
            let tags = QueryTags::new();
            let specs = serde_json::json!([{
                "kind": "http_webhook",
                "url": "ftp://evil.example/guard",
                "fail_behavior": "allow"
            }]);
            let chain =
                build_chain_from_db_specs(&specs, &HashMap::new()).expect("chain should be built");
            let ctx = plan_ctx(&engine, &group, &tags);
            let (actions, outcome) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(is_blocked(&outcome));
            assert_eq!(actions[0].guard, "http_webhook");
            assert!(actions[0]
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("http or https"));
        }
    }

    mod in_memory_metrics {
        use std::sync::Arc;

        use queryflux_core::query::{
            ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryStatus, SqlDialect,
        };
        use queryflux_metrics::{prometheus_store::PrometheusMetrics, QueryRecord};
        use queryflux_persistence::{
            in_memory::InMemoryPersistence, query_history::QueryFilters, QueryHistoryStore,
        };

        fn sample_record() -> QueryRecord {
            QueryRecord {
                proxy_query_id: "q-1".into(),
                backend_query_id: None,
                cluster_group: ClusterGroupName("duckdb-local".to_string()),
                cluster_name: ClusterName("duckdb-1".to_string()),
                cluster_group_config_id: None,
                cluster_config_id: None,
                engine_type: EngineType::DuckDb,
                frontend_protocol: FrontendProtocol::TrinoHttp,
                source_dialect: SqlDialect::Trino,
                target_dialect: SqlDialect::DuckDb,
                was_rewritten: false,
                rewritten_sql: None,
                was_translated: false,
                translation: None,
                translated_sql: None,
                user: None,
                catalog: None,
                database: None,
                sql_preview: "SELECT 1".into(),
                status: QueryStatus::Success,
                routing_trace: None,
                queue_duration_ms: 0,
                execution_duration_ms: 1,
                rows_returned: Some(1),
                error_message: None,
                created_at: chrono::Utc::now(),
                engine_stats: None,
                query_tags: Default::default(),
                query_hash: None,
                query_parameterized_hash: None,
                translated_query_hash: None,
                digest_text: None,
                translated_digest_text: None,
                agent_id: None,
                conversation_id: None,
                step_index: None,
                tool_call_id: None,
                query_intent: None,
                guard_actions: Vec::new(),
                was_guard_blocked: false,
                cache_hit: false,
                worker: None,
            }
        }

        /// Regression test: with in-memory persistence, queries recorded through
        /// the metrics chain must land in the same store the admin API reads
        /// (`/admin/queries`, `/admin/stats`). Wiring Prometheus alone leaves the
        /// dashboard and query history permanently empty.
        #[tokio::test]
        async fn records_query_history_in_backing_store() {
            let prometheus = Arc::new(PrometheusMetrics::new().expect("prometheus init"));
            let mem = Arc::new(InMemoryPersistence::new());
            let metrics = crate::startup::stores::in_memory_metrics(prometheus, mem.clone());

            metrics
                .record_query(sample_record())
                .await
                .expect("record_query");

            // `limit: 0` is the `Default` value and returns nothing; the HTTP
            // layer fills it via serde's `default_limit` (50) instead.
            let filters = QueryFilters {
                limit: 50,
                ..Default::default()
            };
            let rows = mem.list_queries(&filters).await.expect("list_queries");
            assert_eq!(rows.len(), 1, "query must be visible to the admin API");
            assert_eq!(rows[0].sql_preview, "SELECT 1");
        }
    }
}
