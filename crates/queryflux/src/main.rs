use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use queryflux_auth::{
    AllowAllAuthorization, BackendIdentityResolver, LdapAuthProvider, NoneAuthProvider,
    OidcAuthProvider, OpenFgaAuthorizationClient, SimpleAuthorizationPolicy, StaticAuthProvider,
};
use queryflux_cluster_manager::{
    cluster_state::ClusterState, simple::SimpleClusterGroupManager, strategy::strategy_from_config,
};
use queryflux_config::{yaml::YamlFileConfigProvider, ConfigProvider};
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
use queryflux_frontend::{
    admin::{
        build_frontends_status, AdminFrontend, RoutingConfigDto as AdminRoutingConfigDto,
        SecurityConfigDto as AdminSecurityConfigDto, TestClusterFn,
    },
    flight_sql::FlightSqlFrontend,
    mysql_wire::MysqlWireFrontend,
    postgres_wire::PostgresWireFrontend,
    snowflake::SnowflakeFrontend,
    state::LiveConfig,
    trino_http::{state::AppState, TrinoHttpFrontend},
    FrontendListenerTrait,
};
use queryflux_guardrails::{
    built_in::{Guard, ReadOnlyGuard, RequirePredicateGuard, RowLimitGuard},
    config::FailBehavior,
    external::{HttpWebhookGuard, MisconfiguredGuard, PythonScriptGuard},
    GuardChain,
};
use queryflux_metrics::{
    buffered_store::BufferedMetricsStore, prometheus_store::PrometheusMetrics, MetricsStore,
    MultiMetricsStore,
};
use queryflux_persistence::cluster_config::{UpsertClusterConfig, UpsertClusterGroupConfig};
use queryflux_persistence::{
    in_memory::InMemoryPersistence, postgres::PostgresStore, AdminStore, BackendStore,
    DistributedBackendStore, KIND_GUARD,
};
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        compound::CompoundRouter, header::HeaderRouter, protocol_based::ProtocolBasedRouter,
        python_script::PythonScriptRouter, query_regex::QueryRegexRouter, tags::TagsRouter,
    },
    RouterTrait,
};
use queryflux_translation::TranslationService;
use tracing::info;

mod registered_engines;

#[derive(Parser)]
#[command(name = "queryflux", about = "Multi-engine SQL query proxy")]
struct Cli {
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load config before initializing the tracing subscriber so that
    // `otlpEndpoint` from the config file can feed the OTel layer.
    let mut config = YamlFileConfigProvider::new(&cli.config)
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

    info!("QueryFlux starting — loaded config from: {}", cli.config);

    let external_address = config
        .queryflux
        .external_address
        .clone()
        .unwrap_or_else(|| "http://localhost:8080".to_string())
        .trim_end_matches('/')
        .to_string();

    // --- Build persistence + metrics stores (must happen before cluster building) ---
    // When Postgres is configured we seed cluster/group config on first run and read
    // from the DB on subsequent starts, so persistence must be ready before the
    // two-pass cluster/adapter construction below.
    let prometheus = Arc::new(
        PrometheusMetrics::new_with_deny_list(config.queryflux.metrics.tags_deny_list.clone())
            .context("Failed to init Prometheus metrics")?,
    );
    let mut pg_store: Option<Arc<PostgresStore>> = None;
    let mut mem_store: Option<Arc<InMemoryPersistence>> = None;

    let (persistence, metrics): (
        Arc<dyn queryflux_persistence::Persistence>,
        Arc<dyn MetricsStore>,
    ) = match &config.queryflux.persistence {
        queryflux_core::config::PersistenceConfig::Postgres { conn } => {
            let url = conn
                .connection_url()
                .map_err(|m| anyhow::anyhow!("Invalid postgres persistence config: {m}"))?;
            let pg = Arc::new(
                PostgresStore::connect_with_pool_opts(
                    &url,
                    conn.pool_size,
                    conn.acquire_timeout_secs,
                    conn.statement_timeout_secs,
                )
                .await
                .context("Failed to connect to Postgres")?,
            );
            pg.migrate().await.context("Migration failed")?;
            let buffered = Arc::new(BufferedMetricsStore::new(
                pg.clone() as Arc<dyn MetricsStore>,
                100,
                std::time::Duration::from_secs(5),
            ));
            let metrics = Arc::new(MultiMetricsStore::new(vec![
                prometheus.clone() as Arc<dyn MetricsStore>,
                buffered as Arc<dyn MetricsStore>,
            ]));
            pg_store = Some(pg.clone());
            (
                pg as Arc<dyn queryflux_persistence::Persistence>,
                metrics as Arc<dyn MetricsStore>,
            )
        }
        _ => {
            let mem = Arc::new(InMemoryPersistence::new());
            mem_store = Some(mem.clone());
            (
                mem as Arc<dyn queryflux_persistence::Persistence>,
                prometheus.clone() as Arc<dyn MetricsStore>,
            )
        }
    };

    // The durable backend behind the proxy, type-erased so that everything south
    // of this point is wired against traits. A future backend (e.g. Redis) only
    // needs to implement `BackendStore` and be constructed in the match above.
    // `None` in in-memory mode, which intentionally has no durable config source.
    let backend: Option<Arc<dyn BackendStore>> = pg_store.clone().map(|pg| pg as _);
    // Multi-replica coordination is optional: only backends that also implement
    // `DistributedBackendStore` (Postgres today) are stored here.
    let distributed_backend: Option<Arc<dyn DistributedBackendStore>> = pg_store.map(|pg| pg as _);

    // Filled when Postgres loads cluster/group rows — used for query_history FKs on ClusterState.
    let mut cluster_ids_by_name: HashMap<String, i64> = HashMap::new();
    let mut group_ids_by_name: HashMap<String, i64> = HashMap::new();
    // DB cluster records kept for adapter building via build_adapter_from_record.
    let mut startup_cluster_records: Option<
        Vec<queryflux_persistence::cluster_config::ClusterConfigRecord>,
    > = None;

    // --- When Postgres is active, load cluster/group config from DB ---
    // Merge YAML-defined clusters and groups into Postgres on **every** startup when the
    // file declares them (`clusters` / `clusterGroups` non-empty). This keeps Docker/Compose
    // configs authoritative even if the volume already had older rows (e.g. switched engine).
    // **Studio-first** setups omit those maps (or leave them empty) — then nothing is written
    // here and the DB remains the source of truth for those resources.
    if let Some(pg) = &backend {
        if !config.clusters.is_empty() {
            info!("Applying cluster definitions from YAML to Postgres");
            for (name, cfg) in &config.clusters {
                match UpsertClusterConfig::from_core(cfg) {
                    Ok(Some(upsert)) => {
                        pg.upsert_cluster_config(name, &upsert)
                            .await
                            .with_context(|| format!("Upsert cluster '{name}' from YAML"))?;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Err(anyhow::Error::from(e).context(format!(
                            "cluster '{name}': serializing queryAuth for Postgres seed"
                        )));
                    }
                }
            }
        }
        if !config.cluster_groups.is_empty() {
            info!("Applying cluster group definitions from YAML to Postgres");
            for (name, cfg) in &config.cluster_groups {
                pg.upsert_group_config(name, &UpsertClusterGroupConfig::from_core(cfg))
                    .await
                    .with_context(|| format!("Upsert group '{name}' from YAML"))?;
            }
        }

        // Effective config comes from Postgres (YAML above only upserts keys that appear in the file).
        info!("Loading cluster and group configs from Postgres");
        let db_cluster_records = pg
            .list_cluster_configs()
            .await
            .context("Load cluster configs from DB")?;
        cluster_ids_by_name = db_cluster_records
            .iter()
            .map(|r| (r.name.clone(), r.id))
            .collect();
        // Build minimal ClusterConfig values for validation, group resolution, and
        // `BackendIdentityResolver` (`queryAuth`). Adapters are still built from the
        // raw JSONB via `build_adapter_from_record`.
        let mut clusters: HashMap<String, queryflux_core::config::ClusterConfig> = HashMap::new();
        for r in &db_cluster_records {
            let engine = match queryflux_core::engine_registry::parse_engine_key(&r.engine_key) {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(cluster = %r.name, "skipping cluster: {err}");
                    continue;
                }
            };
            let query_auth =
                match queryflux_core::engine_registry::parse_query_auth_from_config_json(&r.config)
                {
                    Ok(qa) => qa,
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!("cluster '{}': invalid queryAuth in JSONB", r.name)
                        });
                    }
                };
            let auth = match queryflux_core::engine_registry::parse_auth_from_config_json(&r.config)
            {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(
                        cluster = %r.name,
                        "invalid auth in cluster config JSON: {e}"
                    );
                    None
                }
            };
            let max_running = max_running_queries_u64_from_db(&r.name, r.max_running_queries)?;
            clusters.insert(
                r.name.clone(),
                queryflux_core::engine_registry::cluster_config_from_persisted_json(
                    engine,
                    r.enabled,
                    max_running,
                    &r.config,
                    auth,
                    query_auth,
                ),
            );
        }
        config.clusters = clusters;
        startup_cluster_records = Some(db_cluster_records);

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
            .map(|r| (r.name.clone(), r.to_core()))
            .collect();

        // Apply persisted security overrides (`security_settings` / `security_config` key).
        if let Ok(Some(v)) = pg.get_proxy_setting("security_config").await {
            let (auth_cfg, authz_cfg) = parse_security_setting(&v);
            if let Some(auth_cfg) = auth_cfg {
                config.auth = auth_cfg;
            }
            if let Some(authz_cfg) = authz_cfg {
                config.authorization = authz_cfg;
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

    // --- Build cluster states and adapters (two-pass) ---
    //
    // Pass 1: iterate `config.clusters`, build one adapter per cluster name.
    // Pass 2: iterate `config.cluster_groups`, resolve members, build ClusterStates.

    type AdapterMap = HashMap<String, queryflux_engine_adapters::AdapterKind>;
    let mut adapters: AdapterMap = HashMap::new();

    // Pass 1 — one adapter per cluster.
    // DB path: build from JSONB records directly; YAML path: build from ClusterConfig.
    if let Some(records) = &startup_cluster_records {
        for record in records {
            if !record.enabled {
                tracing::info!(cluster = %record.name, "Cluster disabled — skipping");
                continue;
            }
            let cluster_name = ClusterName(record.name.clone());
            let placeholder_group = ClusterGroupName("_".to_string());
            match registered_engines::build_adapter_from_record(
                cluster_name,
                placeholder_group,
                &record.engine_key,
                &record.config,
            )
            .await
            {
                Ok(adapter) => {
                    adapters.insert(record.name.clone(), adapter);
                }
                Err(e) => {
                    tracing::error!(
                        cluster = %record.name,
                        error = %e,
                        "Failed to build engine adapter — cluster omitted from routing until config or environment is fixed"
                    );
                }
            }
        }
    } else {
        for (cluster_name_str, cluster_cfg) in &config.clusters {
            if !cluster_cfg.enabled {
                tracing::info!(cluster = %cluster_name_str, "Cluster disabled — skipping");
                continue;
            }
            let cluster_name = ClusterName(cluster_name_str.clone());
            let placeholder_group = ClusterGroupName("_".to_string());
            match registered_engines::build_adapter(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .await
            {
                Ok(adapter) => {
                    adapters.insert(cluster_name_str.clone(), adapter);
                }
                Err(e) => {
                    tracing::error!(
                        cluster = %cluster_name_str,
                        error = %e,
                        "Failed to build engine adapter — cluster omitted from routing until config or environment is fixed"
                    );
                }
            }
        }
    }

    // Pass 2 — one group entry per cluster_group, resolving member cluster names.
    type GroupMap = HashMap<
        ClusterGroupName,
        (
            Vec<Arc<ClusterState>>,
            Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
        ),
    >;
    let mut group_states: GroupMap = HashMap::new();
    let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
    let mut group_order: Vec<String> = Vec::new();

    for (group_name, group_config) in &config.cluster_groups {
        if !group_config.enabled {
            tracing::info!(group = %group_name, "Cluster group disabled — skipping");
            continue;
        }
        let group_key = ClusterGroupName(group_name.clone());
        let mut states: Vec<Arc<ClusterState>> = Vec::new();
        let mut seen_members: HashSet<&str> = HashSet::new();

        for member_name in &group_config.members {
            if !seen_members.insert(member_name.as_str()) {
                tracing::warn!(
                    group = %group_name,
                    cluster = %member_name,
                    "Duplicate cluster in group members list — ignoring extra entry"
                );
                continue;
            }
            let cluster_cfg = config.clusters.get(member_name).context(format!(
                "group '{group_name}' references unknown cluster '{member_name}'"
            ))?;

            if !adapters.contains_key(member_name.as_str()) {
                tracing::warn!(
                    group = %group_name,
                    cluster = %member_name,
                    "Skipping cluster in group: disabled, or adapter failed to build at startup"
                );
                continue;
            }

            let engine = cluster_cfg
                .engine
                .as_ref()
                .context(format!("cluster '{member_name}' missing engine"))?;
            let engine_type = EngineType::from(engine);

            let max_q = cluster_cfg
                .max_running_queries
                .unwrap_or(group_config.max_running_queries);
            let cluster_cid = cluster_ids_by_name.get(member_name).copied();
            let group_cid = group_ids_by_name.get(group_name.as_str()).copied();
            let state = Arc::new(ClusterState::new(
                ClusterName(member_name.clone()),
                group_key.clone(),
                cluster_cid,
                group_cid,
                engine_type,
                cluster_cfg.endpoint.clone(),
                max_q,
                cluster_cfg.enabled,
            ));
            states.push(state);
        }

        let strategy = strategy_from_config(group_config.strategy.as_ref());
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_order.push(group_name.clone());
        group_states.insert(group_key, (states, strategy));
    }
    group_order.sort();

    let health_check_targets = health_targets_from_groups(&group_states, &adapters);
    let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));

    // --- Build translation service ---
    let translation = Arc::new(
        TranslationService::new_sqlglot(config.translation.python_scripts.clone()).unwrap_or_else(
            |e| {
                tracing::warn!("sqlglot unavailable ({e}), translation disabled");
                TranslationService::disabled()
            },
        ),
    );

    // --- Build router chain ---
    let fallback = ClusterGroupName(config.routing_fallback.clone());
    let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();

    for router_cfg in &config.routers {
        use queryflux_core::config::RouterConfig;
        match router_cfg {
            RouterConfig::ProtocolBased {
                trino_http,
                postgres_wire,
                mysql_wire,
                clickhouse_http,
                flight_sql,
                snowflake_http,
                snowflake_sql_api,
            } => {
                routers.push(Box::new(ProtocolBasedRouter {
                    trino_http: trino_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                    postgres_wire: postgres_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                    mysql_wire: mysql_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                    clickhouse_http: clickhouse_http
                        .as_ref()
                        .map(|s| ClusterGroupName(s.clone())),
                    flight_sql: flight_sql.as_ref().map(|s| ClusterGroupName(s.clone())),
                    snowflake_http: snowflake_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                    snowflake_sql_api: snowflake_sql_api
                        .as_ref()
                        .map(|s| ClusterGroupName(s.clone())),
                }));
            }
            RouterConfig::Header {
                header_name,
                header_value_to_group,
            } => {
                let mapping = header_value_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(HeaderRouter::new(header_name.clone(), mapping)));
            }
            RouterConfig::QueryRegex { rules } => {
                let pairs = rules
                    .iter()
                    .map(|r| (r.regex.clone(), r.target_group.clone()))
                    .collect();
                routers.push(Box::new(QueryRegexRouter::new(pairs)));
            }
            RouterConfig::Tags { rules } => {
                routers.push(Box::new(TagsRouter::new(rules.clone())));
            }
            RouterConfig::PythonScript {
                script,
                script_file,
            } => {
                let router = if let Some(path) = script_file {
                    PythonScriptRouter::from_file(path)
                        .context(format!("Failed to load routing script from {path}"))?
                } else {
                    PythonScriptRouter::new(script.clone())
                };
                routers.push(Box::new(router));
            }
            RouterConfig::Compound {
                combine,
                conditions,
                target_group,
            } => {
                routers.push(Box::new(CompoundRouter::new(
                    *combine,
                    conditions.clone(),
                    target_group.clone(),
                )));
            }
            _ => {
                tracing::warn!("Router type not yet implemented, skipping");
            }
        }
    }

    let router_chain = RouterChain::new(routers, fallback);

    let auth_provider = build_auth_provider(&config.auth)?;
    let authorization = build_authorization(&config.authorization, &config.cluster_groups)?;

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

    // --- Startup validation: impersonate only valid for Trino ---
    for (name, cfg) in &config.clusters {
        if matches!(
            cfg.query_auth,
            Some(queryflux_core::config::QueryAuthConfig::Impersonate)
        ) {
            let engine = cfg
                .engine
                .as_ref()
                .map(|e| format!("{e:?}"))
                .unwrap_or_default();
            if !matches!(
                cfg.engine,
                Some(queryflux_core::config::EngineConfig::Trino)
            ) {
                anyhow::bail!(
                    "cluster '{name}': queryAuth.type = impersonate is only supported for Trino, got {engine}"
                );
            }
        }
    }

    let identity_resolver = Arc::new(BackendIdentityResolver::new());
    let cluster_configs = config.clusters.clone();

    let group_translation_scripts: HashMap<String, Vec<String>> = if let Some(pg) = &backend {
        pg.load_group_translation_bodies()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load group translation scripts from Postgres: {e}");
                HashMap::new()
            })
    } else {
        HashMap::new()
    };
    let guard_script_bodies =
        load_guard_script_bodies(backend.as_deref().map(|b| b as &dyn AdminStore)).await;

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

    // --- Startup validation: referential integrity of routing → groups → adapters ---
    {
        let issues = validate_live_config_refs(
            &config.routers,
            &config.routing_fallback,
            &group_members,
            &adapters,
        );
        if !issues.is_empty() {
            for issue in &issues {
                tracing::error!("Config validation: {issue}");
            }
            anyhow::bail!(
                "Startup config has {} referential integrity error(s) — aborting",
                issues.len()
            );
        }
    }

    // --- Wrap hot-reloadable fields in LiveConfig ---
    let group_default_tags: HashMap<String, queryflux_core::tags::QueryTags> = config
        .cluster_groups
        .iter()
        .filter(|(_, g)| !g.default_tags.is_empty())
        .map(|(name, g)| (name.clone(), g.default_tags.clone()))
        .collect();
    let group_cache_settings: HashMap<String, queryflux_core::config::GroupCacheConfig> = config
        .cluster_groups
        .iter()
        .filter_map(|(name, g)| {
            g.cache
                .as_ref()
                .filter(|c| c.enabled)
                .map(|c| (name.clone(), c.clone()))
        })
        .collect();
    let live_config = LiveConfig {
        router_chain,
        guard_chain,
        group_guard_chains,
        cluster_manager,
        adapters,
        health_check_targets,
        cluster_configs,
        group_members,
        group_order,
        group_translation_scripts,
        group_default_tags,
        group_cache_settings,
        auth_provider,
        authorization,
    };
    // Seed the reload cache. When Postgres is active, fingerprint `engine_key` + JSONB config
    // (same format as `build_live_config` on reload) so an engine change rebuilds adapters even
    // when the config blob shape is unchanged. For YAML-only, fold canonical `engine_key` + `ClusterConfig`.
    let initial_config_json: HashMap<String, String> = if let Some(records) =
        &startup_cluster_records
    {
        records
            .iter()
            .map(|r| {
                (
                    r.name.clone(),
                    serde_json::to_string(&(r.engine_key.as_str(), &r.config)).unwrap_or_default(),
                )
            })
            .collect()
    } else {
        live_config
            .cluster_configs
            .iter()
            .map(|(k, v)| {
                let ek = v
                    .engine
                    .as_ref()
                    .map(queryflux_core::engine_registry::engine_key)
                    .unwrap_or("");
                (
                    k.clone(),
                    serde_json::to_string(&(ek, v)).unwrap_or_default(),
                )
            })
            .collect()
    };
    let adapter_reload_cache = Arc::new(tokio::sync::Mutex::new(AdapterReloadCache {
        adapters: live_config.adapters.clone(),
        config_json: initial_config_json,
        // Seed with the initial cluster states so the first reload can inherit health status.
        cluster_states: live_config
            .health_check_targets
            .iter()
            .map(|(_, s)| (s.cluster_name.0.clone(), s.clone()))
            .collect(),
        routing_fallback: config.routing_fallback.clone(),
        routers_cfg: config.routers.clone(),
    }));
    let live = Arc::new(tokio::sync::RwLock::new(live_config));

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
            distributed_backend
                .as_ref()
                .is_some_and(|b| b.supports_distributed_coordination()),
        )
        .map_err(|e| anyhow::anyhow!(e))?;

    if distributed {
        tracing::warn!(
            "Distributed coordination is enabled. \
             This requires HA Postgres — during a Postgres outage the fleet reverts to \
             per-replica capacity limits (group_limit × replicas worst case). \
             Alert on queryflux_coordination_failures_total > 0."
        );
    }

    let capacity_store: Option<Arc<dyn queryflux_persistence::CapacityStore>> = distributed
        .then(|| {
            distributed_backend
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
        .or_else(|| mem_store.map(|m| m as Arc<dyn AdminStore>));
    let security_config = Arc::new(AdminSecurityConfigDto::from_config(
        &config.auth,
        &config.authorization,
        &config.cluster_groups,
    ));
    let routing_config = Arc::new(AdminRoutingConfigDto::from_config(
        &config.routing_fallback,
        &config.routers,
    ));
    let config_reload_notify = Arc::new(tokio::sync::Notify::new());

    let frontends_status = build_frontends_status(
        &config.queryflux.frontends,
        admin_port,
        config.queryflux.external_address.clone(),
    );

    // Build admin credentials — env vars take precedence over YAML.
    let admin_username =
        std::env::var("QUERYFLUX_ADMIN_USER").unwrap_or_else(|_| config.admin_api.username.clone());
    let admin_password = std::env::var("QUERYFLUX_ADMIN_PASSWORD")
        .unwrap_or_else(|_| config.admin_api.password.clone());
    let settings_store = backend
        .clone()
        .map(|b| b as Arc<dyn queryflux_persistence::ProxySettingsStore>);
    let admin_creds = Arc::new(queryflux_auth::AdminCredentialsManager::new(
        admin_username,
        admin_password,
        settings_store,
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

    let admin_store_for_reload = admin_store.clone();
    let cors_origins = config.queryflux.admin_api.cors_allowed_origins.clone();
    if cors_origins.is_empty() {
        tracing::warn!(
            "Admin API CORS allows any origin (corsAllowedOrigins is empty). \
             Set queryflux.adminApi.corsAllowedOrigins to restrict cross-origin access in production."
        );
    }
    let admin = AdminFrontend::new(
        prometheus.clone(),
        live.clone(),
        admin_store,
        admin_port,
        security_config,
        routing_config,
        engine_registry,
        config_reload_notify.clone(),
        frontends_status,
        admin_creds,
        test_cluster_fn,
        cors_origins,
        app_state.result_cache.clone(),
    );

    // --- Start Trino HTTP frontend ---
    let trino_port = config.queryflux.frontends.trino_http.port;
    let frontend = TrinoHttpFrontend::new(
        app_state.clone(),
        trino_port,
        config.queryflux.frontends.trino_http.max_connections,
    );

    info!(
        "QueryFlux ready — Trino HTTP on :{trino_port}, admin/metrics on :{admin_port}, external address: {external_address}"
    );

    if distributed {
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

    // Background task: push cluster utilization snapshots every 5s.
    // In distributed mode, also queries CapacityStore for global running counts.
    //
    // Every replica refreshes its *local* Prometheus gauges (each replica's
    // /metrics is scraped independently), but only the replica holding the
    // sweep lock persists history rows to the backend — otherwise R replicas
    // write R duplicate rows per cluster per tick and Studio's history tables
    // grow R times faster.
    tokio::spawn({
        let state = app_state.clone();
        let prometheus = prometheus.clone();
        let backend = backend.clone();
        let distributed_backend = distributed_backend.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let cluster_manager = state.live.read().await.cluster_manager.clone();
                let Ok(snapshots) = cluster_manager.all_cluster_states().await else {
                    continue;
                };
                let mut records = Vec::with_capacity(snapshots.len());
                for snap in snapshots {
                    // In distributed mode, overlay global running count from CapacityStore
                    // so metrics reflect the true cluster-wide utilization.
                    let global_running = if let Some(cap) = &state.capacity_store {
                        cap.active_count(&snap.cluster_name.0)
                            .await
                            .unwrap_or(snap.running_queries)
                    } else {
                        snap.running_queries
                    };
                    records.push(queryflux_metrics::ClusterSnapshot {
                        cluster_name: snap.cluster_name,
                        group_name: snap.group_name,
                        engine_type: snap.engine_type,
                        running_queries: global_running,
                        queued_queries: snap.queued_queries,
                        max_running_queries: snap.max_running_queries,
                        recorded_at: chrono::Utc::now(),
                    });
                }
                for record in &records {
                    let _ = prometheus.record_cluster_snapshot(record.clone()).await;
                }
                // History rows go to the durable backend. When the backend can
                // coordinate, only the sweep-lock owner persists this cycle; a
                // coordination failure fails open (duplicate rows beat no rows).
                // A non-coordinating backend persists unconditionally — it
                // cannot dedup across replicas anyway.
                let lock = match &distributed_backend {
                    Some(db) => match db.try_sweep_lock("cluster-snapshots").await {
                        Ok(Some(lock)) => Some(Some(lock)),
                        Ok(None) => None, // another replica persists this cycle
                        Err(e) => {
                            tracing::debug!("Snapshot sweep lock failed: {e}");
                            Some(None)
                        }
                    },
                    None => Some(None),
                };
                if let Some(lock) = lock {
                    if let Some(backend) = &backend {
                        for record in records {
                            let _ = backend.record_cluster_snapshot(record).await;
                        }
                    }
                    if let Some(lock) = lock {
                        lock.release().await;
                    }
                }
            }
        }
    });

    // Background task: renew capacity lease heartbeats for this replica every 60s so that
    // long-running queries on a live instance are never reclaimed by `expire_stale` (cutoff
    // is 300s — five missed beats). Leases of crashed replicas stop heartbeating and expire.
    if let Some(cap) = app_state.capacity_store.clone() {
        let state = app_state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Err(e) = cap.heartbeat(&state.instance_id).await {
                    state.metrics.on_coordination_failure("capacity_heartbeat");
                    tracing::warn!("Capacity lease heartbeat failed: {e}");
                }
            }
        });
    }

    // Background task: release capacity for zombie executing queries (client disconnected
    // before polling to completion). Runs every 120s; evicts entries not polled for > 5 min.
    //
    // Uses `last_accessed` from persistence — updated by any proxy instance that handles
    // a poll, throttled to at most one write per 120s. Safe across multiple instances.
    // Also expires stale capacity leases from crashed replicas.
    tokio::spawn({
        let state = app_state.clone();
        let distributed_backend = distributed_backend.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300; // matches Trino's query.client.timeout default
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                interval.tick().await;

                // Single-owner sweep: the eviction and lease expiry below are global
                // (idempotent, but redundant on every replica), so only the replica
                // holding the advisory lock runs them this cycle. A crashed owner's
                // lock is released with its connection, so another replica takes
                // over on its next tick. On lock errors, fail open and sweep anyway.
                let sweep_lock = match &distributed_backend {
                    Some(backend) => match backend.try_sweep_lock("zombie-eviction").await {
                        Ok(Some(lock)) => Some(lock),
                        Ok(None) => continue, // another replica owns this cycle
                        Err(e) => {
                            state.metrics.on_coordination_failure("sweep_lock");
                            tracing::warn!("Sweep lock failed, sweeping anyway: {e}");
                            None
                        }
                    },
                    None => None,
                };

                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);

                // Expire stale capacity leases (crashed replicas).
                if let Some(cap) = &state.capacity_store {
                    match cap.expire_stale(cutoff).await {
                        Ok(0) => {}
                        Ok(n) => tracing::info!("Expired {n} stale capacity leases"),
                        Err(e) => {
                            state.metrics.on_coordination_failure("capacity_expire");
                            tracing::warn!("Capacity lease expiry failed: {e}");
                        }
                    }
                }

                let Ok(all) = state.persistence.list_all().await else {
                    continue;
                };
                for q in all {
                    if q.last_accessed < cutoff {
                        tracing::warn!(
                            id = %q.backend_query_id,
                            cluster = %q.cluster_name,
                            group = %q.cluster_group,
                            last_accessed = %q.last_accessed,
                            "Evicting zombie executing query — not polled for >5 min"
                        );

                        // Best-effort cancel on the backend engine so the query
                        // doesn't keep consuming cluster resources.
                        if let Some(base_url) = &q.poll_base_url {
                            let cancel_url =
                                format!("{base_url}/v1/statement/executing/{}", q.backend_query_id);
                            let client = state.http_client.clone();
                            tokio::spawn(async move {
                                if let Err(e) = client.delete(&cancel_url).send().await {
                                    tracing::debug!(
                                        "Zombie cancel request failed (best-effort): {e}"
                                    );
                                }
                            });
                        }

                        state
                            .metrics
                            .on_query_finished(&q.cluster_group.0, &q.cluster_name.0);
                        let cluster_manager = state.live.read().await.cluster_manager.clone();
                        let _ = cluster_manager
                            .release_cluster(&q.cluster_group, &q.cluster_name)
                            .await;
                        if let Some(cap) = &state.capacity_store {
                            if let Err(e) = cap.release(&q.cluster_name.0, &q.id.0).await {
                                state.metrics.on_coordination_failure("capacity_release");
                                tracing::warn!(
                                    "CapacityStore release failed for zombie query {}: {e}",
                                    q.id
                                );
                            }
                        }
                        let _ = state.persistence.delete(&q.backend_query_id).await;
                    }
                }

                if let Some(lock) = sweep_lock {
                    lock.release().await;
                }
            }
        }
    });

    // Background task: clean up stale queued queries (client disconnected before getting
    // cluster capacity). Runs every 120s;
    // deletes queued entries not accessed for > 5 minutes.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                interval.tick().await;
                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);
                match state
                    .persistence
                    .delete_queued_not_accessed_since(cutoff)
                    .await
                {
                    Ok(0) => {}
                    Ok(n) => tracing::info!("Cleaned up {n} stale queued queries"),
                    Err(e) => tracing::warn!("Queued query cleanup failed: {e}"),
                }
            }
        }
    });

    // Background task: periodically clean up expired cache entries.
    {
        let cache = app_state.result_cache.clone();
        let interval_secs = config
            .queryflux
            .cache_backend
            .as_ref()
            .map(|c| c.cleanup_interval_secs)
            .unwrap_or(300);
        if interval_secs > 0 {
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    match cache.cleanup_expired().await {
                        Ok(0) => {}
                        Ok(n) => {
                            tracing::info!(deleted = n, "Cache cleanup: removed expired entries")
                        }
                        Err(e) => tracing::warn!(error = %e, "Cache cleanup failed"),
                    }
                }
            });
        }
    }

    // Background task: enforce query_history_retention_days — runs hourly and deletes
    // query_records rows older than the configured retention window.
    // Only active when Postgres is configured and retention_days is set.
    if let (Some(backend), Some(retention_days)) = (
        backend.clone(),
        config.queryflux.query_history_retention_days,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            interval.tick().await; // skip the first immediate tick at startup
            loop {
                interval.tick().await;
                let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
                match backend.purge_old_query_records(cutoff).await {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!("Purged {n} query records older than {retention_days} days")
                    }
                    Err(e) => tracing::warn!("Query history purge failed: {e}"),
                }
            }
        });
    }

    // Background task: hot-reload routing rules + cluster configs from the DB when:
    //   1. Another replica bumps the config revision (distributed LISTEN/NOTIFY via ConfigRevisionStore)
    //   2. This replica's admin API writes config (local tokio::sync::Notify fast-path)
    //   3. A periodic timer fires (safety-net polling, configurable via configReloadIntervalSecs)
    //
    // When no durable backend is configured, only local Notify triggers guard-chain reloads.
    tokio::spawn({
        let live = live.clone();
        let backend = backend.clone();
        let cache = adapter_reload_cache.clone();
        let notify = config_reload_notify.clone();
        let admin_for_reload = admin_store_for_reload;
        let periodic_secs = config.queryflux.periodic_config_reload_interval_secs();

        // Subscribe to distributed config revision changes (push where the
        // backend supports it, e.g. Postgres LISTEN/NOTIFY).
        let revision_rx = if let Some(backend) = &backend {
            match backend.subscribe_revisions().await {
                Ok(Some(rx)) => {
                    tracing::info!("Subscribed to backend config revision notifications");
                    Some(rx)
                }
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!("Failed to subscribe to config revision notifications: {e}");
                    None
                }
            }
        } else {
            None
        };

        async move {
            async fn do_reload(
                backend: &Arc<dyn BackendStore>,
                cache: &tokio::sync::Mutex<AdapterReloadCache>,
                live: &Arc<tokio::sync::RwLock<LiveConfig>>,
            ) {
                let mut cache_guard = cache.lock().await;
                // Snapshot the pieces a reload must never silently weaken; the
                // read guard is dropped before the write below.
                let prev = {
                    let l = live.read().await;
                    PreservedLive {
                        auth_provider: l.auth_provider.clone(),
                        authorization: l.authorization.clone(),
                        guard_chain: l.guard_chain.clone(),
                        group_guard_chains: l.group_guard_chains.clone(),
                    }
                };
                match reload_live_config(backend, &mut cache_guard, &prev).await {
                    Ok(new_live) => {
                        *live.write().await = new_live;
                        tracing::info!("Live config reloaded from backend");
                    }
                    Err(e) => tracing::warn!("Config reload failed: {e}"),
                }
            }

            async fn reload_guard_chain_from_admin(
                admin: &Option<Arc<dyn AdminStore>>,
                live: &Arc<tokio::sync::RwLock<LiveConfig>>,
            ) {
                if let Some(store) = admin {
                    let guard_script_bodies =
                        load_guard_script_bodies_from_admin(store.as_ref()).await;
                    match store.get_proxy_setting("guardrails_config").await {
                        Ok(Some(v)) => {
                            let (global, groups) =
                                build_guard_chains_from_db_value(&v, &guard_script_bodies);
                            let mut w = live.write().await;
                            w.guard_chain = global;
                            w.group_guard_chains = groups;
                        }
                        Ok(None) => {
                            let mut w = live.write().await;
                            w.guard_chain = None;
                            w.group_guard_chains = HashMap::new();
                        }
                        Err(e) => tracing::warn!("Guard chain reload failed: {e}"),
                    }
                }
            }

            async fn do_reload_or_guard(
                backend: &Option<Arc<dyn BackendStore>>,
                cache: &tokio::sync::Mutex<AdapterReloadCache>,
                live: &Arc<tokio::sync::RwLock<LiveConfig>>,
                admin: &Option<Arc<dyn AdminStore>>,
            ) {
                if let Some(backend) = backend {
                    do_reload(backend, cache, live).await;
                } else {
                    // YAML-mode reload contract: without a Postgres backend, routing rules,
                    // cluster configs, and adapters are fixed at startup from the YAML file
                    // and cannot change at runtime. Only guard chains (stored in the admin
                    // store) can be hot-reloaded via the admin API. Routing or cluster
                    // changes in YAML require a process restart.
                    reload_guard_chain_from_admin(admin, live).await;
                }
            }

            // Wrap the optional receiver so we can always select on it.
            let mut revision_rx = revision_rx;

            // Coalesce notification bursts: a bulk admin save bumps the revision
            // once per write, and each bump is one channel message — without
            // draining, N rapid writes would trigger N full reloads (adapter
            // rebuilds included) on every replica. A short settle window lets
            // writes a few hundred ms apart collapse into one reload too.
            async fn coalesce_revisions(rx: &mut Option<tokio::sync::mpsc::Receiver<u64>>) {
                if let Some(rx) = rx.as_mut() {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    while rx.try_recv().is_ok() {}
                }
            }

            // A future that resolves when the revision receiver gets a message,
            // or pends forever if there is no receiver. A closed channel (the
            // LISTEN/NOTIFY task died) drops the receiver so we don't spin on
            // an immediately-ready `recv()`; periodic polling remains the
            // safety net for config propagation.
            async fn recv_revision(rx: &mut Option<tokio::sync::mpsc::Receiver<u64>>) -> u64 {
                match rx {
                    Some(r) => match r.recv().await {
                        Some(rev) => rev,
                        None => {
                            tracing::warn!(
                                "Config revision channel closed — falling back to periodic polling"
                            );
                            *rx = None;
                            std::future::pending().await
                        }
                    },
                    None => std::future::pending().await,
                }
            }

            match periodic_secs {
                None => loop {
                    tokio::select! {
                        _ = notify.notified() => {
                            tracing::debug!("Config reload triggered by local admin write");
                        }
                        rev = recv_revision(&mut revision_rx) => {
                            tracing::debug!(revision = rev, "Config reload triggered by distributed revision change");
                        }
                    }
                    coalesce_revisions(&mut revision_rx).await;
                    do_reload_or_guard(&backend, &cache, &live, &admin_for_reload).await;
                },
                Some(interval_secs) => {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                    interval.tick().await; // skip the first immediate tick — startup already loaded
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {}
                            _ = notify.notified() => {
                                tracing::debug!("Config reload triggered by local admin write");
                            }
                            rev = recv_revision(&mut revision_rx) => {
                                tracing::debug!(revision = rev, "Config reload triggered by distributed revision change");
                            }
                        }
                        coalesce_revisions(&mut revision_rx).await;
                        do_reload_or_guard(&backend, &cache, &live, &admin_for_reload).await;
                    }
                }
            }
        }
    });

    // Background task: health-check each cluster every 30s via its adapter.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let targets = {
                    let live = state.live.read().await;
                    live.health_check_targets.clone()
                };
                for (adapter, state) in &targets {
                    let healthy = adapter.health_check().await;
                    if !healthy {
                        tracing::warn!(
                            cluster = %state.cluster_name.0,
                            group = %state.group_name.0,
                            "Health check failed — marking cluster unhealthy"
                        );
                    } else if !state.is_healthy() {
                        tracing::info!(
                            cluster = %state.cluster_name.0,
                            group = %state.group_name.0,
                            "Health check recovered — marking cluster healthy"
                        );
                    }
                    state.set_healthy(healthy);
                }
            }
        }
    });

    // Background task: reconcile in-memory running_queries counters with ground truth
    // from each engine (engines that implement fetch_running_query_count). Runs every 30s.
    // Corrects drift caused by proxy crashes, client disconnects, or any other leak.
    // In distributed mode, local counters are a cache; CapacityStore is authoritative.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let targets = {
                    let live = state.live.read().await;
                    live.health_check_targets.clone()
                };
                for (adapter, cstate) in &targets {
                    // In distributed mode, sync local counter from CapacityStore (global truth).
                    if let Some(cap) = &state.capacity_store {
                        if let Ok(global) = cap.active_count(&cstate.cluster_name.0).await {
                            cstate.set_running_queries(global);
                            continue;
                        }
                    }

                    let tracked = cstate.running_queries();
                    let max = cstate.max_running_queries();
                    if tracked > max {
                        let fix = adapter.fetch_running_query_count().await.unwrap_or(0);
                        tracing::warn!(
                            cluster = %cstate.cluster_name.0,
                            group = %cstate.group_name.0,
                            tracked,
                            max,
                            fix,
                            "running_queries above group capacity; resetting from engine count"
                        );
                        cstate.set_running_queries(fix);
                        continue;
                    }
                    if let Some(actual) = adapter.fetch_running_query_count().await {
                        if actual != tracked {
                            tracing::info!(
                                cluster = %cstate.cluster_name.0,
                                group = %cstate.group_name.0,
                                tracked,
                                actual,
                                "Reconciling running_queries counter with engine ground truth"
                            );
                            cstate.set_running_queries(actual);
                        }
                    }
                }
            }
        }
    });

    // Spawn all enabled frontends as tasks. Each frontend observes `shutdown_rx`
    // internally: axum-based frontends use `with_graceful_shutdown` (stop accepting,
    // finish in-flight requests), wire-based frontends break their accept loop, and
    // tonic (Flight SQL) uses `serve_with_shutdown`.
    let mut trino_handle = tokio::spawn({
        let fe = frontend;
        let rx = shutdown_rx.clone();
        async move { fe.listen(rx).await }
    });
    let mut admin_handle = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move { admin.listen(rx).await }
    });
    let mut mysql_handle = tokio::spawn({
        let state = app_state.clone();
        let rx = shutdown_rx.clone();
        let cfg = config.queryflux.frontends.mysql_wire.clone();
        async move {
            match cfg {
                Some(c) if c.enabled => {
                    MysqlWireFrontend::new(state, c.port, c.max_connections)
                        .listen(rx)
                        .await
                }
                _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
            }
        }
    });
    let mut postgres_handle = tokio::spawn({
        let state = app_state.clone();
        let rx = shutdown_rx.clone();
        let cfg = config.queryflux.frontends.postgres_wire.clone();
        async move {
            match cfg {
                Some(c) if c.enabled => {
                    PostgresWireFrontend::new(state, c.port, c.max_connections)
                        .listen(rx)
                        .await
                }
                _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
            }
        }
    });
    let mut flight_sql_handle = tokio::spawn({
        let state = app_state.clone();
        let rx = shutdown_rx.clone();
        let cfg = config.queryflux.frontends.flight_sql.clone();
        async move {
            match cfg {
                Some(c) if c.enabled => {
                    FlightSqlFrontend::new(state, c.port, c.max_connections)
                        .listen(rx)
                        .await
                }
                _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
            }
        }
    });
    let mut snowflake_handle = tokio::spawn({
        let state = app_state.clone();
        let rx = shutdown_rx.clone();
        let cfg = config.queryflux.frontends.snowflake_http.clone();
        async move {
            match cfg {
                Some(c) if c.enabled => SnowflakeFrontend::new(state, c).listen(rx).await,
                _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
            }
        }
    });

    // Wait for either a shutdown signal or an unexpected frontend exit.
    let shutdown_signal = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("register SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => tracing::info!("Received SIGINT — initiating graceful shutdown"),
                _ = sigterm.recv() => tracing::info!("Received SIGTERM — initiating graceful shutdown"),
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            tracing::info!("Received Ctrl-C — initiating graceful shutdown");
        }
    };

    tokio::select! {
        _ = shutdown_signal => {},
        r = &mut trino_handle   => { if let Ok(Err(e)) = r { tracing::error!("Trino HTTP exited unexpectedly: {e}"); } },
        r = &mut admin_handle   => { if let Ok(Err(e)) = r { tracing::error!("Admin exited unexpectedly: {e}"); } },
        r = &mut mysql_handle   => { if let Ok(Err(e)) = r { tracing::error!("MySQL wire exited unexpectedly: {e}"); } },
        r = &mut postgres_handle => { if let Ok(Err(e)) = r { tracing::error!("Postgres wire exited unexpectedly: {e}"); } },
        r = &mut flight_sql_handle => { if let Ok(Err(e)) = r { tracing::error!("Flight SQL exited unexpectedly: {e}"); } },
        r = &mut snowflake_handle => { if let Ok(Err(e)) = r { tracing::error!("Snowflake exited unexpectedly: {e}"); } },
    }

    // --- Phase 1: signal all frontends to stop accepting new connections ---
    let _ = shutdown_tx.send(true);

    // --- Phase 2: drain in-flight requests ---
    let drain_timeout_secs = config.queryflux.shutdown_drain_timeout_secs();
    let drain_timeout = std::time::Duration::from_secs(drain_timeout_secs);
    tracing::info!("Draining in-flight requests (timeout: {drain_timeout_secs}s)...");

    let drain_future = async {
        // Wait for all frontends to finish processing in-flight requests.
        // Axum frontends complete when all connections are done; wire frontends
        // return immediately from their accept loop but spawned connection tasks
        // continue running.
        let _ = tokio::join!(
            trino_handle,
            admin_handle,
            mysql_handle,
            postgres_handle,
            flight_sql_handle,
            snowflake_handle,
        );

        // Poll persistence until no executing or queued queries remain (or until
        // the outer timeout fires). This covers spawned wire-protocol connection
        // handlers that are still mid-query after the accept loop exited.
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            interval.tick().await;
            let executing = app_state
                .persistence
                .list_all()
                .await
                .map(|v| v.len())
                .unwrap_or(0);
            let queued = app_state
                .persistence
                .list_queued()
                .await
                .map(|v| v.len())
                .unwrap_or(0);
            if executing == 0 && queued == 0 {
                tracing::info!("All in-flight queries drained");
                break;
            }
            tracing::info!(executing, queued, "Waiting for queries to drain...");
        }
    };

    if tokio::time::timeout(drain_timeout, drain_future)
        .await
        .is_err()
    {
        let executing = app_state
            .persistence
            .list_all()
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        let queued = app_state
            .persistence
            .list_queued()
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        tracing::warn!(
            executing,
            queued,
            "Drain timeout reached after {drain_timeout_secs}s — forcing shutdown"
        );
    }

    // --- Phase 3: release capacity leases owned by this replica ---
    tracing::info!("Releasing capacity leases for this replica...");
    if let Some(cap) = &app_state.capacity_store {
        if let Err(e) = cap.release_all_for_instance(&app_state.instance_id).await {
            tracing::warn!("Failed to release capacity leases on shutdown: {e}");
        } else {
            tracing::info!("Capacity leases released");
        }
    }

    tracing::info!("QueryFlux shutdown complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Hot-reload helpers
// ---------------------------------------------------------------------------

type GroupStatesMap = HashMap<
    ClusterGroupName,
    (
        Vec<Arc<ClusterState>>,
        Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
    ),
>;

/// Convert optional Postgres `BIGINT` (`max_running_queries`) to `Option<u64>`.
/// Negative values fail fast (invalid row).
fn max_running_queries_u64_from_db(cluster: &str, v: Option<i64>) -> Result<Option<u64>> {
    match v {
        None => Ok(None),
        Some(n) => u64::try_from(n).map(Some).map_err(|_| {
            anyhow::anyhow!(
                "cluster '{cluster}': max_running_queries must be non-negative (got {n})"
            )
        }),
    }
}

/// Holds adapter instances between DB reloads. Adapters are recreated when the
/// reload fingerprint changes (`engine_key` + config JSON), so engine switches and
/// endpoint/credential updates rebuild adapters.
struct AdapterReloadCache {
    adapters: HashMap<String, queryflux_engine_adapters::AdapterKind>,
    config_json: HashMap<String, String>,
    /// Previous-generation cluster states keyed by cluster name.
    /// Preserved across reloads so that health status and running-query counters
    /// are not reset to their initial values every time the config is reloaded.
    cluster_states: HashMap<String, Arc<ClusterState>>,
    /// Last-known routing from DB (or YAML at startup). Used when `load_routing_config` returns
    /// `Ok(None)` so periodic reload does not wipe routing.
    routing_fallback: String,
    routers_cfg: Vec<queryflux_core::config::RouterConfig>,
}

fn health_targets_from_groups(
    group_states: &GroupStatesMap,
    adapters: &HashMap<String, queryflux_engine_adapters::AdapterKind>,
) -> Vec<(queryflux_engine_adapters::AdapterKind, Arc<ClusterState>)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (states, _) in group_states.values() {
        for state in states {
            let name = state.cluster_name.0.clone();
            if seen.insert(name.clone()) {
                if let Some(adapter) = adapters.get(&name) {
                    out.push((adapter.clone(), state.clone()));
                }
            }
        }
    }
    out
}

/// Validate referential integrity of a config that is about to go live.
///
/// Returns a list of human-readable issue strings (empty = valid). Callers should
/// treat any non-empty return as a fatal config error and keep the previous LiveConfig.
///
/// Checks:
///  - `routing_fallback` names a group that exists in `group_members`.
///  - Every static `target_group` reference in `routers_cfg` names a group in `group_members`.
///    (PythonScript routers are skipped — their target group is computed at runtime.)
///  - Every cluster name listed in `group_members` has a built adapter in `adapters`.
fn validate_live_config_refs(
    routers_cfg: &[queryflux_core::config::RouterConfig],
    routing_fallback: &str,
    group_members: &HashMap<String, Vec<String>>,
    adapters: &HashMap<String, queryflux_engine_adapters::AdapterKind>,
) -> Vec<String> {
    use queryflux_core::config::RouterConfig;

    let mut issues: Vec<String> = Vec::new();

    if !group_members.contains_key(routing_fallback) {
        issues.push(format!(
            "routing_fallback references unknown group '{routing_fallback}'"
        ));
    }

    for router in routers_cfg {
        let mut refs: Vec<&str> = Vec::new();
        match router {
            RouterConfig::ProtocolBased {
                trino_http,
                postgres_wire,
                mysql_wire,
                clickhouse_http,
                flight_sql,
                snowflake_http,
                snowflake_sql_api,
            } => {
                let opts: [Option<&str>; 7] = [
                    trino_http.as_deref(),
                    postgres_wire.as_deref(),
                    mysql_wire.as_deref(),
                    clickhouse_http.as_deref(),
                    flight_sql.as_deref(),
                    snowflake_http.as_deref(),
                    snowflake_sql_api.as_deref(),
                ];
                refs.extend(opts.into_iter().flatten());
            }
            RouterConfig::Header {
                header_value_to_group,
                ..
            } => {
                refs.extend(header_value_to_group.values().map(String::as_str));
            }
            RouterConfig::UserGroup { user_to_group } => {
                refs.extend(user_to_group.values().map(String::as_str));
            }
            RouterConfig::QueryRegex { rules } => {
                refs.extend(rules.iter().map(|r| r.target_group.as_str()));
            }
            RouterConfig::Tags { rules } => {
                refs.extend(rules.iter().map(|r| r.target_group.as_str()));
            }
            RouterConfig::Compound { target_group, .. } => {
                refs.push(target_group.as_str());
            }
            RouterConfig::PythonScript { .. } => {}
        }
        for group in refs {
            if !group_members.contains_key(group) {
                issues.push(format!("router references unknown group '{group}'"));
            }
        }
    }

    for (group, members) in group_members {
        for member in members {
            if !adapters.contains_key(member.as_str()) {
                issues.push(format!(
                    "group '{group}' member '{member}' has no built adapter"
                ));
            }
        }
    }

    issues
}

/// Build a `LiveConfig` from DB cluster records, group maps, and router chain components.
///
/// This is the DB load path: adapters are built directly from the JSONB config blob
/// in each `ClusterConfigRecord`, bypassing the `ClusterConfig` god struct.
///
/// `cache` holds adapter instances from the previous generation. Adapters are reused
/// only when the fingerprint of `engine_key` + JSONB config matches the previous reload;
/// otherwise they are rebuilt (e.g. engine switch, endpoint, or password changed).
#[allow(clippy::too_many_arguments)]
async fn build_live_config(
    cluster_records: &[queryflux_persistence::cluster_config::ClusterConfigRecord],
    cluster_groups: &std::collections::HashMap<String, queryflux_core::config::ClusterGroupConfig>,
    cluster_ids_by_name: &HashMap<String, i64>,
    group_ids_by_name: &HashMap<String, i64>,
    routers_cfg: &[queryflux_core::config::RouterConfig],
    routing_fallback: &str,
    group_translation_scripts: HashMap<String, Vec<String>>,
    cache: &mut AdapterReloadCache,
) -> Result<LiveConfig> {
    use queryflux_cluster_manager::{
        cluster_state::ClusterState, simple::SimpleClusterGroupManager,
        strategy::strategy_from_config,
    };
    use queryflux_core::engine_registry::{
        cluster_config_from_persisted_json, json_str, parse_auth_from_config_json,
        parse_engine_key, parse_query_auth_from_config_json,
    };
    use queryflux_core::tags::QueryTags;

    // Build a lookup map from records for group member resolution.
    let records_by_name: HashMap<
        &str,
        &queryflux_persistence::cluster_config::ClusterConfigRecord,
    > = cluster_records
        .iter()
        .map(|r| (r.name.as_str(), r))
        .collect();

    let prev_config_json = cache.config_json.clone();

    // Build adapters — reuse when serialized cluster config is unchanged.
    for record in cluster_records {
        let cluster_name_str = &record.name;
        if !record.enabled {
            cache.adapters.remove(cluster_name_str.as_str());
            cache.config_json.remove(cluster_name_str.as_str());
            continue;
        }
        let cfg_json = serde_json::to_string(&(record.engine_key.as_str(), &record.config))
            .unwrap_or_default();
        let reuse = cache.adapters.contains_key(cluster_name_str.as_str())
            && prev_config_json
                .get(cluster_name_str.as_str())
                .map(String::as_str)
                == Some(cfg_json.as_str());
        if reuse {
            continue;
        }
        cache.adapters.remove(cluster_name_str.as_str());
        cache.config_json.remove(cluster_name_str.as_str());

        let cluster_name = ClusterName(cluster_name_str.clone());
        let placeholder_group = ClusterGroupName("_".to_string());
        let adapter = match registered_engines::build_adapter_from_record(
            cluster_name,
            placeholder_group,
            &record.engine_key,
            &record.config,
        )
        .await
        {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(
                    cluster = %cluster_name_str,
                    error = %e,
                    "Reload: failed to build engine adapter — cluster omitted until fixed"
                );
                continue;
            }
        };
        cache.adapters.insert(cluster_name_str.clone(), adapter);
        cache.config_json.insert(cluster_name_str.clone(), cfg_json);
    }
    cache
        .adapters
        .retain(|name, _| records_by_name.contains_key(name.as_str()));
    cache
        .config_json
        .retain(|name, _| records_by_name.contains_key(name.as_str()));

    // Build group states.
    let mut group_states: GroupStatesMap = HashMap::new();
    let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
    let mut group_order: Vec<String> = Vec::new();

    for (group_name, group_config) in cluster_groups {
        if !group_config.enabled {
            continue;
        }
        let group_key = ClusterGroupName(group_name.clone());
        let mut states: Vec<Arc<ClusterState>> = Vec::new();
        let mut seen_members: HashSet<&str> = HashSet::new();

        for member_name in &group_config.members {
            if !seen_members.insert(member_name.as_str()) {
                tracing::warn!(
                    group = %group_name,
                    cluster = %member_name,
                    "Reload: duplicate cluster in group members — ignoring extra entry"
                );
                continue;
            }
            let record = match records_by_name.get(member_name.as_str()) {
                Some(r) => r,
                None => {
                    tracing::warn!(group = %group_name, cluster = %member_name, "Reload: group references unknown cluster");
                    continue;
                }
            };
            if !cache.adapters.contains_key(member_name.as_str()) {
                tracing::info!(group = %group_name, cluster = %member_name, "Reload: skipping disabled/missing cluster in group");
                continue;
            }
            let engine = match parse_engine_key(&record.engine_key) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let engine_type = EngineType::from(&engine);
            let max_q = max_running_queries_u64_from_db(member_name, record.max_running_queries)?
                .unwrap_or(group_config.max_running_queries);
            let endpoint = json_str(&record.config, "endpoint");
            let cluster_cid = cluster_ids_by_name.get(member_name.as_str()).copied();
            let group_cid = group_ids_by_name.get(group_name.as_str()).copied();

            // When the JSONB + engine_key fingerprint is unchanged, rebuild `ClusterState` from
            // the current record anyway (group membership, IDs, endpoint, max_q may still change)
            // but copy health and queue counters from the previous generation.
            let cfg_json = serde_json::to_string(&(record.engine_key.as_str(), &record.config))
                .unwrap_or_default();
            let config_unchanged = prev_config_json
                .get(member_name.as_str())
                .map(String::as_str)
                == Some(cfg_json.as_str());

            let state = Arc::new(ClusterState::new(
                ClusterName(member_name.clone()),
                group_key.clone(),
                cluster_cid,
                group_cid,
                engine_type,
                endpoint,
                max_q,
                record.enabled,
            ));
            if let Some(prev) = cache.cluster_states.get(member_name.as_str()) {
                let snap = prev.snapshot();
                state.set_healthy(snap.is_healthy);
                if config_unchanged {
                    state.set_running_queries(snap.running_queries);
                    state.set_queued_queries(snap.queued_queries);
                }
            }
            states.push(state);
        }

        let strategy = strategy_from_config(group_config.strategy.as_ref());
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_order.push(group_name.clone());
        group_states.insert(group_key, (states, strategy));
    }
    group_order.sort();

    let health_check_targets = health_targets_from_groups(&group_states, &cache.adapters);
    cache.cluster_states = health_check_targets
        .iter()
        .map(|(_, s)| (s.cluster_name.0.clone(), s.clone()))
        .collect();
    let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));

    // Build minimal ClusterConfig values for BackendIdentityResolver (`queryAuth` from JSONB).
    let mut cluster_configs: HashMap<String, queryflux_core::config::ClusterConfig> =
        HashMap::new();
    for r in cluster_records {
        let engine = match parse_engine_key(&r.engine_key) {
            Ok(e) => e,
            Err(err) => {
                tracing::warn!(cluster = %r.name, "reload: {err}");
                continue;
            }
        };
        let query_auth = parse_query_auth_from_config_json(&r.config).map_err(|e| {
            anyhow::anyhow!("cluster '{}': invalid queryAuth in JSONB: {e}", r.name)
        })?;
        let auth = match parse_auth_from_config_json(&r.config) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(
                    cluster = %r.name,
                    "reload: invalid auth in cluster config JSON: {e}"
                );
                None
            }
        };
        let max_running = max_running_queries_u64_from_db(&r.name, r.max_running_queries)?;
        cluster_configs.insert(
            r.name.clone(),
            cluster_config_from_persisted_json(
                engine,
                r.enabled,
                max_running,
                &r.config,
                auth,
                query_auth,
            ),
        );
    }

    // Build router chain.
    let fallback = ClusterGroupName(routing_fallback.to_string());
    let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();
    for router_cfg in routers_cfg {
        use queryflux_core::config::RouterConfig;
        match router_cfg {
            RouterConfig::ProtocolBased {
                trino_http,
                postgres_wire,
                mysql_wire,
                clickhouse_http,
                flight_sql,
                snowflake_http,
                snowflake_sql_api,
            } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::protocol_based::ProtocolBasedRouter {
                        trino_http: trino_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                        postgres_wire: postgres_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                        mysql_wire: mysql_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                        clickhouse_http: clickhouse_http
                            .as_ref()
                            .map(|s| ClusterGroupName(s.clone())),
                        flight_sql: flight_sql.as_ref().map(|s| ClusterGroupName(s.clone())),
                        snowflake_http: snowflake_http
                            .as_ref()
                            .map(|s| ClusterGroupName(s.clone())),
                        snowflake_sql_api: snowflake_sql_api
                            .as_ref()
                            .map(|s| ClusterGroupName(s.clone())),
                    },
                ));
            }
            RouterConfig::Header {
                header_name,
                header_value_to_group,
            } => {
                let mapping = header_value_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(
                    queryflux_routing::implementations::header::HeaderRouter::new(
                        header_name.clone(),
                        mapping,
                    ),
                ));
            }
            RouterConfig::QueryRegex { rules } => {
                let pairs = rules
                    .iter()
                    .map(|r| (r.regex.clone(), r.target_group.clone()))
                    .collect();
                routers.push(Box::new(
                    queryflux_routing::implementations::query_regex::QueryRegexRouter::new(pairs),
                ));
            }
            RouterConfig::Tags { rules } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::tags::TagsRouter::new(rules.clone()),
                ));
            }
            RouterConfig::PythonScript {
                script,
                script_file,
            } => {
                let router = if let Some(path) = script_file {
                    match queryflux_routing::implementations::python_script::PythonScriptRouter::from_file(path) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!("Reload: failed to load routing script from {path}: {e}");
                            continue;
                        }
                    }
                } else {
                    queryflux_routing::implementations::python_script::PythonScriptRouter::new(
                        script.clone(),
                    )
                };
                routers.push(Box::new(router));
            }
            RouterConfig::Compound {
                combine,
                conditions,
                target_group,
            } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::compound::CompoundRouter::new(
                        *combine,
                        conditions.clone(),
                        target_group.clone(),
                    ),
                ));
            }
            _ => {
                tracing::warn!("Reload: router type not yet implemented, skipping");
            }
        }
    }
    let router_chain = RouterChain::new(routers, fallback);

    let group_default_tags: HashMap<String, QueryTags> = cluster_groups
        .iter()
        .filter(|(_, g)| !g.default_tags.is_empty())
        .map(|(name, g)| (name.clone(), g.default_tags.clone()))
        .collect();

    let group_cache_settings: HashMap<String, queryflux_core::config::GroupCacheConfig> =
        cluster_groups
            .iter()
            .filter_map(|(name, g)| {
                g.cache
                    .as_ref()
                    .filter(|c| c.enabled)
                    .map(|c| (name.clone(), c.clone()))
            })
            .collect();

    // Referential integrity: routers must target groups that exist in this reload cycle,
    // and every declared group member must have a built adapter. A stale read (e.g. a
    // group deleted between the cluster read and the group read) would produce a live
    // config where dispatch returns NoClusterGroupAvailable with no explanation.
    // On any inconsistency, bail out so the caller keeps the previous LiveConfig.
    let issues = validate_live_config_refs(
        routers_cfg,
        routing_fallback,
        &group_members,
        &cache.adapters,
    );
    if !issues.is_empty() {
        for issue in &issues {
            tracing::warn!("Live config validation: {issue}");
        }
        return Err(anyhow::anyhow!(
            "Live config is internally inconsistent ({} issue(s)); keeping previous config",
            issues.len()
        ));
    }

    Ok(LiveConfig {
        router_chain,
        guard_chain: None,
        group_guard_chains: HashMap::new(),
        cluster_manager,
        adapters: cache.adapters.clone(),
        health_check_targets,
        cluster_configs,
        group_members,
        group_order,
        group_translation_scripts,
        group_default_tags,
        group_cache_settings,
        auth_provider: Arc::new(NoneAuthProvider::new(false)),
        authorization: Arc::new(AllowAllAuthorization),
    })
}

/// Load cluster/group configs + routing config from Postgres and build a fresh `LiveConfig`.
/// Existing adapter instances are reused for clusters that haven't changed.
///
/// Cluster records are passed directly to `build_live_config` — no `to_core()` conversion.
/// Hot pieces carried over from the previous `LiveConfig` when their backing
/// rows are absent from the backend (never configured via admin) or fail to
/// parse. A reload must never revert auth to permissive defaults or drop
/// YAML-configured guard chains just because no row was ever written.
struct PreservedLive {
    auth_provider: Arc<dyn queryflux_auth::AuthProvider>,
    authorization: Arc<dyn queryflux_auth::AuthorizationChecker>,
    guard_chain: Option<Arc<GuardChain>>,
    group_guard_chains: HashMap<String, Arc<GuardChain>>,
}

async fn reload_live_config(
    pg: &Arc<dyn BackendStore>,
    cache: &mut AdapterReloadCache,
    prev: &PreservedLive,
) -> Result<LiveConfig> {
    let cluster_records = pg
        .list_cluster_configs()
        .await
        .context("reload: list_cluster_configs")?;
    let cluster_ids_by_name: HashMap<String, i64> = cluster_records
        .iter()
        .map(|r| (r.name.clone(), r.id))
        .collect();

    let group_records = pg
        .list_group_configs()
        .await
        .context("reload: list_group_configs")?;
    let group_ids_by_name: HashMap<String, i64> = group_records
        .iter()
        .map(|r| (r.name.clone(), r.id))
        .collect();
    let cluster_groups: std::collections::HashMap<
        String,
        queryflux_core::config::ClusterGroupConfig,
    > = group_records
        .into_iter()
        .map(|r| (r.name.clone(), r.to_core()))
        .collect();

    // Load routing from DB if present; otherwise keep last-known routing (startup YAML or previous DB load).
    let (routing_fallback, routers_cfg) = match pg.load_routing_config().await {
        Ok(Some(loaded)) => {
            let mut routers = Vec::new();
            for v in loaded.routers {
                match serde_json::from_value::<queryflux_core::config::RouterConfig>(v) {
                    Ok(r) => routers.push(r),
                    Err(e) => {
                        tracing::warn!(error = %e, "Reload: skipping invalid routing_rules row")
                    }
                }
            }
            cache.routing_fallback = loaded.routing_fallback.clone();
            cache.routers_cfg.clone_from(&routers);
            (loaded.routing_fallback, routers)
        }
        Ok(None) => (cache.routing_fallback.clone(), cache.routers_cfg.clone()),
        Err(e) => {
            return Err(anyhow::anyhow!("reload: load_routing_config: {e}"));
        }
    };

    let group_translation_scripts = pg
        .load_group_translation_bodies()
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "reload: load_group_translation_bodies failed");
            HashMap::new()
        });
    let guard_script_bodies = load_guard_script_bodies(Some(pg.as_ref() as &dyn AdminStore)).await;

    let mut live = build_live_config(
        &cluster_records,
        &cluster_groups,
        &cluster_ids_by_name,
        &group_ids_by_name,
        &routers_cfg,
        &routing_fallback,
        group_translation_scripts,
        cache,
    )
    .await?;

    // Carry forward the pieces build_live_config seeds with placeholders. The
    // DB reads below only *override* these on success — a missing row or a
    // parse failure keeps the previous (startup-YAML or last-good) values.
    live.auth_provider = prev.auth_provider.clone();
    live.authorization = prev.authorization.clone();
    live.guard_chain = prev.guard_chain.clone();
    live.group_guard_chains = prev.group_guard_chains.clone();

    // Guardrails from DB (UI-managed) override carried-over chains. An admin
    // "clear" still writes an empty `global` row, so Ok(None) can only mean
    // "never configured via admin" — keep the previous (e.g. YAML) chains.
    match pg.get_proxy_setting("guardrails_config").await {
        Ok(Some(v)) => {
            let (global, groups) = build_guard_chains_from_db_value(&v, &guard_script_bodies);
            live.guard_chain = global;
            live.group_guard_chains = groups;
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("Reload: guardrails_config read failed; keeping previous chains: {e}")
        }
    }

    // Rebuild auth/authz from persisted security config. On a missing row or
    // any parse/build failure keep the carried-over providers — a reload must
    // never fall back to permissive defaults.
    match pg.get_proxy_setting("security_config").await {
        Ok(Some(v)) => {
            let (auth_cfg, authz_cfg) = parse_security_setting(&v);
            match auth_cfg.map(|cfg| build_auth_provider(&cfg)) {
                Some(Ok(provider)) => live.auth_provider = provider,
                Some(Err(e)) => {
                    tracing::warn!("Reload: failed to rebuild auth provider; keeping previous: {e}")
                }
                None => tracing::warn!(
                    "Reload: security_config has no recognizable auth section; keeping previous"
                ),
            }
            match authz_cfg.map(|cfg| build_authorization(&cfg, &cluster_groups)) {
                Some(Ok(checker)) => live.authorization = checker,
                Some(Err(e)) => {
                    tracing::warn!("Reload: failed to rebuild authorization; keeping previous: {e}")
                }
                None => tracing::warn!(
                    "Reload: security_config has no recognizable authorization section; keeping previous"
                ),
            }
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("Reload: security_config read failed; keeping previous auth: {e}")
        }
    }

    Ok(live)
}

/// Parse the persisted `security_config` proxy setting into typed configs.
///
/// `PUT /admin/config/security` stores the flat `UpsertSecurityConfig` shape
/// (`auth_provider`, `auth_required`, `authorization_provider`, ...); earlier
/// builds wrapped typed configs under `authConfig` / `authorizationConfig`.
/// Accept both so existing rows keep working. Returns `None` for a section
/// that is absent or fails to parse.
fn parse_security_setting(
    v: &serde_json::Value,
) -> (
    Option<queryflux_core::config::AuthConfig>,
    Option<queryflux_core::config::AuthorizationConfig>,
) {
    use serde_json::{json, Value};

    let auth = if let Some(wrapped) = v.get("authConfig") {
        serde_json::from_value(wrapped.clone()).ok()
    } else if v.get("auth_provider").is_some() {
        serde_json::from_value(json!({
            "provider": v.get("auth_provider").cloned().unwrap_or(Value::Null),
            "required": v.get("auth_required").cloned().unwrap_or(Value::Bool(false)),
            "oidc": v.get("oidc").cloned().unwrap_or(Value::Null),
            "ldap": v.get("ldap").cloned().unwrap_or(Value::Null),
            "staticUsers": v.get("static_users").cloned().unwrap_or(Value::Null),
        }))
        .ok()
    } else {
        None
    };

    let authz = if let Some(wrapped) = v.get("authorizationConfig") {
        serde_json::from_value(wrapped.clone()).ok()
    } else if v.get("authorization_provider").is_some() {
        serde_json::from_value(json!({
            "provider": v.get("authorization_provider").cloned().unwrap_or(Value::Null),
            "openfga": v.get("openfga").cloned().unwrap_or(Value::Null),
        }))
        .ok()
    } else {
        None
    };

    (auth, authz)
}

fn build_auth_provider(
    auth: &queryflux_core::config::AuthConfig,
) -> Result<Arc<dyn queryflux_auth::AuthProvider>> {
    use queryflux_core::config::AuthProviderConfig;
    let auth_required = auth.required;
    Ok(match &auth.provider {
        AuthProviderConfig::None => {
            info!("Auth provider: none (network-trust only)");
            Arc::new(NoneAuthProvider::new(auth_required))
        }
        AuthProviderConfig::Static => {
            let users = auth
                .static_users
                .as_ref()
                .context("auth.provider = static requires auth.staticUsers to be configured")?
                .users
                .clone();
            info!(user_count = users.len(), "Auth provider: static");
            Arc::new(StaticAuthProvider::new(users, auth_required))
        }
        AuthProviderConfig::Oidc => {
            let oidc_cfg = auth
                .oidc
                .clone()
                .context("auth.provider = oidc requires auth.oidc to be configured")?;
            info!(issuer = %oidc_cfg.issuer, "Auth provider: OIDC");
            Arc::new(OidcAuthProvider::new(oidc_cfg, auth_required))
        }
        AuthProviderConfig::Ldap => {
            let ldap_cfg = auth
                .ldap
                .clone()
                .context("auth.provider = ldap requires auth.ldap to be configured")?;
            info!(url = %ldap_cfg.url, "Auth provider: LDAP");
            Arc::new(LdapAuthProvider::new(ldap_cfg, auth_required))
        }
    })
}

fn build_authorization(
    authz: &queryflux_core::config::AuthorizationConfig,
    cluster_groups: &HashMap<String, queryflux_core::config::ClusterGroupConfig>,
) -> Result<Arc<dyn queryflux_auth::AuthorizationChecker>> {
    use queryflux_core::config::AuthorizationProviderConfig;
    Ok(match &authz.provider {
        AuthorizationProviderConfig::None => {
            let policies = cluster_groups
                .iter()
                .map(|(name, cfg)| (name.clone(), cfg.authorization.clone()))
                .collect();
            let has_any_policy = cluster_groups.values().any(|cfg| {
                !cfg.authorization.allow_groups.is_empty()
                    || !cfg.authorization.allow_users.is_empty()
            });
            if has_any_policy {
                info!("Authorization: simple allow-list policy");
                Arc::new(SimpleAuthorizationPolicy::new(policies))
            } else {
                info!("Authorization: allow-all (no allow-lists configured)");
                Arc::new(AllowAllAuthorization)
            }
        }
        AuthorizationProviderConfig::OpenFga => {
            let openfga_cfg = authz.openfga.clone().context(
                "authorization.provider = openfga requires authorization.openfga to be configured",
            )?;
            info!(url = %openfga_cfg.url, store_id = %openfga_cfg.store_id, "Authorization: OpenFGA");
            Arc::new(OpenFgaAuthorizationClient::new(openfga_cfg))
        }
    })
}

async fn load_guard_script_bodies(store: Option<&dyn AdminStore>) -> HashMap<i64, String> {
    let Some(store) = store else {
        return HashMap::new();
    };
    load_guard_script_bodies_from_admin(store).await
}

async fn load_guard_script_bodies_from_admin(admin: &dyn AdminStore) -> HashMap<i64, String> {
    admin
        .list_user_scripts(Some(KIND_GUARD))
        .await
        .map(|scripts| scripts.into_iter().map(|s| (s.id, s.body)).collect())
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to load guard scripts from persistence: {e}");
            HashMap::new()
        })
}

fn resolve_python_guard_script(
    inline_script: Option<String>,
    script_id: Option<i64>,
    timeout_ms: Option<u64>,
    guard_script_bodies: &HashMap<i64, String>,
) -> Box<dyn Guard> {
    if let Some(script) = inline_script.filter(|s| !s.trim().is_empty()) {
        return Box::new(PythonScriptGuard { script, timeout_ms });
    }
    if let Some(script_id) = script_id {
        if let Some(script) = guard_script_bodies.get(&script_id) {
            return Box::new(PythonScriptGuard {
                script: script.clone(),
                timeout_ms,
            });
        }
        return Box::new(MisconfiguredGuard {
            guard_name: "python_script",
            reason: format!("python_script guard references missing guard script id {script_id}"),
        });
    }
    Box::new(MisconfiguredGuard {
        guard_name: "python_script",
        reason: "python_script guard requires either script or script_id".to_string(),
    })
}

fn make_http_webhook_guard(
    url: String,
    timeout_ms: Option<u64>,
    retry_count: u32,
    fail_behavior: FailBehavior,
    headers: HashMap<String, String>,
) -> Box<dyn Guard> {
    if url.trim().is_empty() {
        tracing::warn!("http_webhook guard has empty URL; using MisconfiguredGuard");
        Box::new(MisconfiguredGuard {
            guard_name: "http_webhook",
            reason: "http_webhook guard is missing required field \"url\"".to_string(),
        })
    } else {
        Box::new(HttpWebhookGuard {
            url,
            timeout_ms,
            retry_count,
            fail_behavior,
            headers,
            client: reqwest::Client::new(),
        })
    }
}

/// Build YAML guard specs into a `GuardChain`. Returns `None` when the list is empty
/// or contains only unrecognised entries.
fn build_chain_from_yaml_specs(
    specs: &[queryflux_core::config::GuardSpecConfig],
    guard_script_bodies: &HashMap<i64, String>,
) -> Option<Arc<GuardChain>> {
    use queryflux_core::config::{GuardFailBehaviorConfig, GuardKindConfig};
    let mut guards: Vec<Box<dyn Guard>> = Vec::new();
    for spec in specs {
        match &spec.kind {
            GuardKindConfig::BuiltIn => {
                let Some(name) = spec.name.as_deref() else {
                    tracing::error!("built_in guard is missing required field \"name\"; skipping");
                    continue;
                };
                match name {
                    "read_only" => guards.push(Box::new(ReadOnlyGuard)),
                    "row_limit" => guards.push(Box::new(RowLimitGuard {
                        max_rows: spec.max_rows,
                    })),
                    "require_predicate" => guards.push(Box::new(RequirePredicateGuard {
                        applies_to: spec.applies_to.clone().unwrap_or_default(),
                    })),
                    other => tracing::warn!(name = other, "Unknown built-in guard name; skipping"),
                }
            }
            GuardKindConfig::PythonScript => {
                let guard = resolve_python_guard_script(
                    spec.script.clone(),
                    spec.script_id,
                    spec.timeout_ms,
                    guard_script_bodies,
                );
                guards.push(guard);
            }
            GuardKindConfig::HttpWebhook => {
                guards.push(make_http_webhook_guard(
                    spec.url.clone().unwrap_or_default(),
                    spec.timeout_ms,
                    spec.retry_count.unwrap_or(0),
                    match spec.fail_behavior {
                        Some(GuardFailBehaviorConfig::Allow) => FailBehavior::Allow,
                        _ => FailBehavior::Deny,
                    },
                    spec.headers.clone().unwrap_or_default(),
                ));
            }
        }
    }
    if guards.is_empty() {
        None
    } else {
        Some(Arc::new(GuardChain::new(guards)))
    }
}

/// Build global + per-group guard chains from the YAML `guardrails:` section.
fn build_guard_chains(
    config: &queryflux_core::config::ProxyConfig,
    guard_script_bodies: &HashMap<i64, String>,
) -> (Option<Arc<GuardChain>>, HashMap<String, Arc<GuardChain>>) {
    let Some(cfg) = config.guardrails.as_ref() else {
        return (None, HashMap::new());
    };
    let global = build_chain_from_yaml_specs(&cfg.global, guard_script_bodies);
    let groups = cfg
        .groups
        .iter()
        .filter_map(|(name, specs)| {
            build_chain_from_yaml_specs(specs, guard_script_bodies)
                .map(|chain| (name.clone(), chain))
        })
        .collect();
    (global, groups)
}

/// Build DB guard specs (kind string format) into a `GuardChain`.
fn build_chain_from_db_specs(
    specs: &serde_json::Value,
    guard_script_bodies: &HashMap<i64, String>,
) -> Option<Arc<GuardChain>> {
    struct DbGuardSpec {
        kind: String,
        name: Option<String>,
        max_rows: Option<u64>,
        applies_to: Option<Vec<String>>,
        script_id: Option<i64>,
        script: Option<String>,
        url: Option<String>,
        timeout_ms: Option<u64>,
        retry_count: Option<u32>,
        fail_behavior: Option<String>,
        headers: Option<HashMap<String, String>>,
    }
    fn parse_spec(item: &serde_json::Value) -> Option<DbGuardSpec> {
        let o = item.as_object()?;
        Some(DbGuardSpec {
            kind: o.get("kind")?.as_str()?.to_string(),
            name: o
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            max_rows: o.get("max_rows").and_then(|v| v.as_u64()),
            applies_to: o.get("applies_to").and_then(|v| v.as_array()).map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str().map(|s| s.to_string()))
                    .collect()
            }),
            script_id: o.get("script_id").and_then(|v| v.as_i64()),
            script: o.get("script").and_then(|v| v.as_str()).map(str::to_string),
            url: o.get("url").and_then(|v| v.as_str()).map(str::to_string),
            timeout_ms: o.get("timeout_ms").and_then(|v| v.as_u64()),
            retry_count: o
                .get("retry_count")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            fail_behavior: o
                .get("fail_behavior")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            headers: o.get("headers").and_then(|v| v.as_object()).map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            }),
        })
    }
    let arr = specs.as_array()?;
    let mut guards: Vec<Box<dyn Guard>> = Vec::new();
    for item in arr {
        let Some(spec) = parse_spec(item) else {
            continue;
        };
        match spec.kind.as_str() {
            "built_in" => {
                let name = spec.name.as_deref().unwrap_or("");
                match name {
                    "read_only" => guards.push(Box::new(ReadOnlyGuard)),
                    "row_limit" => guards.push(Box::new(RowLimitGuard {
                        max_rows: spec.max_rows,
                    })),
                    "require_predicate" => guards.push(Box::new(RequirePredicateGuard {
                        applies_to: spec.applies_to.unwrap_or_default(),
                    })),
                    other => tracing::warn!(name = other, "Unknown built-in guard name; skipping"),
                }
            }
            "http_webhook" => {
                guards.push(make_http_webhook_guard(
                    spec.url.unwrap_or_default(),
                    spec.timeout_ms,
                    spec.retry_count.unwrap_or(0),
                    match spec.fail_behavior.as_deref() {
                        Some("allow") => FailBehavior::Allow,
                        _ => FailBehavior::Deny,
                    },
                    spec.headers.unwrap_or_default(),
                ));
            }
            "python_script" => {
                let guard = resolve_python_guard_script(
                    spec.script,
                    spec.script_id,
                    spec.timeout_ms,
                    guard_script_bodies,
                );
                guards.push(guard);
            }
            other => tracing::warn!(kind = other, "Unknown guard kind; skipping"),
        }
    }
    if guards.is_empty() {
        None
    } else {
        Some(Arc::new(GuardChain::new(guards)))
    }
}

/// Build global + per-group guard chains from the flat JSON format stored by the Studio UI.
///
/// The DB format mirrors `GuardrailsConfig` from the TypeScript API types:
/// `{ global: GuardSpecDto[], groups: Record<string, GuardSpecDto[]> }`.
fn build_guard_chains_from_db_value(
    v: &serde_json::Value,
    guard_script_bodies: &HashMap<i64, String>,
) -> (Option<Arc<GuardChain>>, HashMap<String, Arc<GuardChain>>) {
    let Some(obj) = v.as_object() else {
        return (None, HashMap::new());
    };
    let global_val = obj
        .get("global")
        .cloned()
        .unwrap_or(serde_json::Value::Array(vec![]));
    let global = build_chain_from_db_specs(&global_val, guard_script_bodies);

    let groups = obj
        .get("groups")
        .and_then(|g| g.as_object())
        .map(|groups_obj| {
            groups_obj
                .iter()
                .filter_map(|(name, specs)| {
                    build_chain_from_db_specs(specs, guard_script_bodies)
                        .map(|chain| (name.clone(), chain))
                })
                .collect()
        })
        .unwrap_or_default();

    (global, groups)
}

#[cfg(test)]
mod tests {
    use super::parse_security_setting;
    use queryflux_core::config::{AuthProviderConfig, AuthorizationProviderConfig};
    use serde_json::json;

    /// The shape `PUT /admin/config/security` actually persists
    /// (flat `UpsertSecurityConfig` fields).
    #[test]
    fn parse_security_setting_flat_admin_shape() {
        let v = json!({
            "auth_provider": "static",
            "auth_required": true,
            "oidc": null,
            "ldap": null,
            "static_users": { "users": { "alice": { "password": "pw" } } },
            "authorization_provider": "none",
            "openfga": null,
        });
        let (auth, authz) = parse_security_setting(&v);
        let auth = auth.expect("auth section should parse");
        assert!(matches!(auth.provider, AuthProviderConfig::Static));
        assert!(auth.required);
        assert!(auth.static_users.is_some());
        let authz = authz.expect("authz section should parse");
        assert!(matches!(authz.provider, AuthorizationProviderConfig::None));
    }

    /// Legacy wrapped shape from earlier builds.
    #[test]
    fn parse_security_setting_wrapped_legacy_shape() {
        let v = json!({
            "authConfig": { "provider": "none", "required": true },
            "authorizationConfig": { "provider": "openfga", "openfga": {
                "url": "http://fga:8080", "storeId": "s1", "model": null
            }},
        });
        let (auth, authz) = parse_security_setting(&v);
        let auth = auth.expect("wrapped auth should parse");
        assert!(matches!(auth.provider, AuthProviderConfig::None));
        assert!(auth.required);
        let authz = authz.expect("wrapped authz should parse");
        assert!(matches!(
            authz.provider,
            AuthorizationProviderConfig::OpenFga
        ));
    }

    /// Unrecognizable value: both sections None so the caller preserves
    /// the previous providers instead of weakening to permissive defaults.
    #[test]
    fn parse_security_setting_unrecognized_yields_none() {
        let (auth, authz) = parse_security_setting(&json!({ "something": "else" }));
        assert!(auth.is_none());
        assert!(authz.is_none());
    }
}
