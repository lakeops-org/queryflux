use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
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
use queryflux_persistence::{
    in_memory::InMemoryPersistence, postgres::PostgresStore, AdminStore, BackendStore,
    DistributedBackendStore, KIND_GUARD,
};
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        compound::CompoundRouter, header::HeaderRouter, protocol_based::ProtocolBasedRouter,
        python_script::PythonScriptRouter, query_regex::QueryRegexRouter, tags::TagsRouter,
        user_group::UserGroupRouter,
    },
    RouterTrait,
};
use queryflux_translation::TranslationService;
use tracing::info;

mod registered_engines;

/// Returns `true` when the interval fired (continue work), `false` on shutdown.
async fn tick_or_shutdown(
    interval: &mut tokio::time::Interval,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = shutdown.wait_for(|v| *v) => false,
        _ = interval.tick() => true,
    }
}

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
                PostgresStore::connect_from_config(&url, conn)
                    .await
                    .context("Failed to connect to Postgres")?,
            );
            if conn.auto_migrate {
                pg.migrate().await.context("Migration failed")?;
            } else {
                info!(
                    "persistence.autoMigrate=false — skipping startup migrations \
                     (apply with `queryflux migrate` or a Job)"
                );
            }
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
        queryflux_core::config::PersistenceConfig::Redis { url } => {
            anyhow::bail!(
                "persistence.type = redis is not implemented (configured url: {url}). \
                 Use type: postgres or type: inMemory."
            );
        }
        queryflux_core::config::PersistenceConfig::InMemory => {
            let mem = Arc::new(InMemoryPersistence::new());
            mem_store = Some(mem.clone());
            (
                mem.clone() as Arc<dyn queryflux_persistence::Persistence>,
                in_memory_metrics(prometheus.clone(), mem),
            )
        }
    };

    // The durable backend behind the proxy, type-erased so that everything south
    // of this point is wired against traits. Redis is intentionally rejected above
    // until a real `BackendStore` implementation exists (no silent in-memory fallback).
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
            let base_cfg = queryflux_core::engine_registry::cluster_config_from_persisted_json(
                engine.clone(),
                r.enabled,
                max_running,
                &r.config,
                auth.clone(),
                query_auth.clone(),
            );

            // Expand variants: insert a ClusterConfig for each expanded name.
            let variants = parse_cluster_variants(&r.variants);
            if variants.is_empty() {
                let mut base_cfg = base_cfg;
                queryflux_core::config::apply_default_probe_queries(
                    &mut base_cfg,
                    &r.engine_key,
                    &r.config,
                );
                clusters.insert(r.name.clone(), base_cfg);
            } else {
                match queryflux_core::config::expand_cluster_variants(
                    &r.name,
                    &r.config,
                    &r.engine_key,
                    &variants,
                    base_cfg.health_check_query.as_deref(),
                    base_cfg.reconcile_query.as_deref(),
                ) {
                    Ok(expanded) => {
                        for exp in expanded {
                            let mut variant_cfg = base_cfg.clone();
                            variant_cfg.max_running_queries =
                                exp.max_running_queries.or(max_running);
                            variant_cfg.health_check_query = exp.health_check_query;
                            variant_cfg.reconcile_query = exp.reconcile_query;
                            clusters.insert(exp.expanded_name, variant_cfg);
                        }
                    }
                    Err(err) => {
                        tracing::error!(cluster = %r.name, error = %err, "Variant expansion failed — cluster omitted");
                    }
                }
            }
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

    // --- Build cluster states and adapters (two-pass) ---
    //
    // Pass 1: iterate `config.clusters`, build one adapter per cluster name.
    // Pass 2: iterate `config.cluster_groups`, resolve members, build ClusterStates.

    type AdapterMap = HashMap<String, queryflux_engine_adapters::AdapterKind>;
    let mut adapters: AdapterMap = HashMap::new();

    // Pass 1 — one adapter per cluster (expanding variants for DB path).
    // DB path: build from JSONB records directly; YAML path: build from ClusterConfig.
    if let Some(records) = &startup_cluster_records {
        for record in records {
            if !record.enabled {
                tracing::info!(cluster = %record.name, "Cluster disabled — skipping");
                continue;
            }

            // Parse variants from the JSONB column.
            let variants = parse_cluster_variants(&record.variants);
            let (health_check_query, reconcile_query) = extract_base_probe_queries(&record.config);

            if variants.is_empty() {
                // No variants — build a single adapter as before.
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
            } else {
                // Expand variants into independent runtime clusters.
                let expanded = match queryflux_core::config::expand_cluster_variants(
                    &record.name,
                    &record.config,
                    &record.engine_key,
                    &variants,
                    health_check_query.as_deref(),
                    reconcile_query.as_deref(),
                ) {
                    Ok(e) => e,
                    Err(err) => {
                        tracing::error!(cluster = %record.name, error = %err, "Variant expansion failed — cluster omitted");
                        continue;
                    }
                };
                for exp in &expanded {
                    // Map expanded name back to parent DB ID.
                    cluster_ids_by_name.insert(exp.expanded_name.clone(), record.id);

                    let cluster_name = ClusterName(exp.expanded_name.clone());
                    let placeholder_group = ClusterGroupName("_".to_string());
                    match registered_engines::build_adapter_from_record(
                        cluster_name,
                        placeholder_group,
                        &record.engine_key,
                        &exp.merged_config,
                    )
                    .await
                    {
                        Ok(adapter) => {
                            adapters.insert(exp.expanded_name.clone(), adapter);
                        }
                        Err(e) => {
                            tracing::error!(
                                cluster = %exp.expanded_name,
                                error = %e,
                                "Failed to build engine adapter for variant — omitted"
                            );
                        }
                    }
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

        let strategy = strategy_from_config(group_config.strategy.as_ref())
            .context(format!("group '{group_name}' cluster-selection strategy"))?;
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_order.push(group_name.clone());
        group_states.insert(group_key, (states, strategy));
    }
    group_order.sort();

    let health_check_targets = health_targets_from_groups(&group_states, &adapters);
    let initial_strategies: HashMap<
        String,
        Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
    > = group_states
        .iter()
        .map(|(name, (_, strategy))| (name.0.clone(), strategy.clone()))
        .collect();
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
            RouterConfig::UserGroup { user_to_group } => {
                let mapping = user_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(UserGroupRouter::new(mapping)));
            }
            RouterConfig::QueryRegex { rules } => {
                routers.push(Box::new(QueryRegexRouter::from_rules(rules.clone())));
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
        }
    }

    let router_chain = RouterChain::new(routers, fallback);

    config
        .validate_startup_security()
        .map_err(|e| anyhow::anyhow!("Startup security validation failed: {e}"))?;

    let auth_provider = build_auth_provider(&config.auth)?;
    let authorization = build_authorization(
        &config.authorization,
        &config.cluster_groups,
        operators_from_auth(&config.auth),
    )?;

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
    let group_max_queued_queries: HashMap<String, Option<u64>> = config
        .cluster_groups
        .iter()
        .filter(|(_, g)| g.max_queued_queries.is_some())
        .map(|(name, g)| (name.clone(), g.max_queued_queries))
        .collect();
    let group_capacity_wait_timeout_secs: HashMap<String, u64> = config
        .cluster_groups
        .iter()
        .map(|(name, g)| (name.clone(), g.capacity_wait_timeout_secs_or_default()))
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
    // Build custom health/reconcile query maps from cluster configs.
    let mut custom_health_queries: HashMap<String, String> = HashMap::new();
    let mut custom_reconcile_queries: HashMap<String, String> = HashMap::new();
    for (name, cfg) in &cluster_configs {
        if let Some(q) = &cfg.health_check_query {
            custom_health_queries.insert(name.clone(), q.clone());
        }
        if let Some(q) = &cfg.reconcile_query {
            custom_reconcile_queries.insert(name.clone(), q.clone());
        }
    }

    let live_config = LiveConfig {
        router_chain,
        guard_chain,
        group_guard_chains,
        cluster_manager,
        adapters,
        health_check_targets,
        custom_health_queries,
        custom_reconcile_queries,
        cluster_configs,
        group_members,
        group_order,
        group_translation_scripts,
        group_default_tags,
        group_max_queued_queries,
        group_capacity_wait_timeout_secs,
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
        let mut m: HashMap<String, String> = HashMap::new();
        for r in records {
            let variants = parse_cluster_variants(&r.variants);
            if variants.is_empty() {
                m.insert(
                    r.name.clone(),
                    serde_json::to_string(&(r.engine_key.as_str(), &r.config)).unwrap_or_default(),
                );
            } else {
                let (health_check_query, reconcile_query) = extract_base_probe_queries(&r.config);
                match queryflux_core::config::expand_cluster_variants(
                    &r.name,
                    &r.config,
                    &r.engine_key,
                    &variants,
                    health_check_query.as_deref(),
                    reconcile_query.as_deref(),
                ) {
                    Ok(expanded) => {
                        for exp in expanded {
                            m.insert(
                                exp.expanded_name,
                                serde_json::to_string(&(r.engine_key.as_str(), &exp.merged_config))
                                    .unwrap_or_default(),
                            );
                        }
                    }
                    Err(err) => {
                        tracing::error!(cluster = %r.name, error = %err, "Variant expansion failed — cluster omitted");
                    }
                }
            }
        }
        m
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
        strategies: initial_strategies,
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
        app_state.clone(),
    );

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
    // In distributed mode, overlay the last published engine reconcile counts so
    // every replica's metrics reflect the same backend ground truth.
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
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let cluster_manager = state.live.read().await.cluster_manager.clone();
                let Ok(snapshots) = cluster_manager.all_cluster_states().await else {
                    continue;
                };
                let mut records = Vec::with_capacity(snapshots.len());
                for snap in snapshots {
                    let global_running = if let Some(db) = &distributed_backend {
                        let store = db.clone() as Arc<dyn queryflux_persistence::CapacityStore>;
                        store
                            .active_count(&snap.cluster_name.0)
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
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
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
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300; // matches Trino's query.client.timeout default
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }

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

                        // Best-effort cancel on the backend with cluster credentials
                        // (same path as client/admin cancel). The previous unauthenticated
                        // DELETE on `/v1/statement/executing/...` did not stop secured Trino.
                        let backend_id = q.backend_query_id.clone();
                        let wire_auth = q.wire_auth.clone();
                        let cluster = q.cluster_name.0.clone();
                        let (engine_type, tgt_dialect) =
                            if let Some(adapter) = state.adapter(&cluster).await {
                                (adapter.engine_type(), adapter.translation_target_dialect())
                            } else {
                                state.engine_type_for_cluster(&cluster).await
                            };
                        state.record_executing_cancelled(
                            &q,
                            queryflux_core::query::FrontendProtocol::TrinoHttp,
                            engine_type,
                            tgt_dialect,
                            "zombie_evicted: not polled for >5 min",
                        );
                        if let Some(adapter) = state.adapter(&cluster).await {
                            tokio::spawn(async move {
                                if let Err(e) =
                                    adapter.cancel_query(&backend_id, wire_auth.as_ref()).await
                                {
                                    tracing::debug!(
                                        "Zombie cancel request failed (best-effort): {e}"
                                    );
                                }
                            });
                        } else {
                            tracing::debug!(
                                cluster = %cluster,
                                id = %q.backend_query_id,
                                "No adapter available for zombie cancel"
                            );
                        }

                        state
                            .release_query_slot(&q.cluster_group, &q.cluster_name, &q.id.0)
                            .await;
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
        let distributed_backend = distributed_backend.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }

                let sweep_lock = match &distributed_backend {
                    Some(backend) => match backend.try_sweep_lock("stale-queued-eviction").await {
                        Ok(Some(lock)) => Some(lock),
                        Ok(None) => continue,
                        Err(e) => {
                            state.metrics.on_coordination_failure("sweep_lock");
                            tracing::warn!("Stale queued sweep lock failed, sweeping anyway: {e}");
                            None
                        }
                    },
                    None => None,
                };

                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);
                let Ok(queued) = state.persistence.list_queued().await else {
                    if let Some(lock) = sweep_lock {
                        lock.release().await;
                    }
                    continue;
                };
                let mut cleaned = 0u64;
                for q in queued {
                    if q.last_accessed < cutoff {
                        if let Ok(Some(taken)) = state.persistence.take_queued(&q.id).await {
                            state.record_queued_terminal(
                                &taken,
                                queryflux_core::query::QueryStatus::Failed,
                                "stale_queued_evicted: client disconnected before dispatch",
                            );
                            cleaned += 1;
                        }
                    }
                }
                if cleaned > 0 {
                    tracing::info!("Cleaned up {cleaned} stale queued queries");
                }

                if let Some(lock) = sweep_lock {
                    lock.release().await;
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
            let mut shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    return;
                }
                loop {
                    if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                        break;
                    }
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
    // query_records, query_digest_stats, and cluster_snapshots older than the window.
    // Only active when Postgres is configured and retention_days is set.
    if let (Some(backend), Some(retention_days)) = (
        backend.clone(),
        config.queryflux.query_history_retention_days,
    ) {
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                return;
            }
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
                match backend.purge_old_query_records(cutoff).await {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!("Purged {n} history rows older than {retention_days} days")
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
        let metrics = app_state.metrics.clone();
        let periodic_secs = config.queryflux.periodic_config_reload_interval_secs();
        let mut shutdown_rx = shutdown_rx.clone();

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
                metrics: &Arc<dyn MetricsStore>,
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
                match reload_live_config(backend, &mut cache_guard, &prev, metrics).await {
                    Ok(new_live) => {
                        *live.write().await = new_live;
                        tracing::info!("Live config reloaded from backend");
                    }
                    Err(e) => {
                        metrics.on_config_reload_failure("reload");
                        tracing::warn!("Config reload failed: {e}");
                    }
                }
            }

            async fn reload_guard_chain_from_admin(
                admin: &Option<Arc<dyn AdminStore>>,
                live: &Arc<tokio::sync::RwLock<LiveConfig>>,
                metrics: &Arc<dyn MetricsStore>,
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
                        Err(e) => {
                            metrics.on_config_reload_failure("guard_reload");
                            tracing::warn!("Guard chain reload failed: {e}");
                        }
                    }
                }
            }

            async fn do_reload_or_guard(
                backend: &Option<Arc<dyn BackendStore>>,
                cache: &tokio::sync::Mutex<AdapterReloadCache>,
                live: &Arc<tokio::sync::RwLock<LiveConfig>>,
                admin: &Option<Arc<dyn AdminStore>>,
                metrics: &Arc<dyn MetricsStore>,
            ) {
                if let Some(backend) = backend {
                    do_reload(backend, cache, live, metrics).await;
                } else {
                    // YAML-mode reload contract: without a Postgres backend, routing rules,
                    // cluster configs, and adapters are fixed at startup from the YAML file
                    // and cannot change at runtime. Only guard chains (stored in the admin
                    // store) can be hot-reloaded via the admin API. Routing or cluster
                    // changes in YAML require a process restart.
                    reload_guard_chain_from_admin(admin, live, metrics).await;
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
                        _ = shutdown_rx.wait_for(|v| *v) => break,
                        _ = notify.notified() => {
                            tracing::debug!("Config reload triggered by local admin write");
                        }
                        rev = recv_revision(&mut revision_rx) => {
                            tracing::debug!(revision = rev, "Config reload triggered by distributed revision change");
                        }
                    }
                    coalesce_revisions(&mut revision_rx).await;
                    do_reload_or_guard(&backend, &cache, &live, &admin_for_reload, &metrics).await;
                },
                Some(interval_secs) => {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                    interval.tick().await; // skip the first immediate tick — startup already loaded
                    loop {
                        tokio::select! {
                            _ = shutdown_rx.wait_for(|v| *v) => break,
                            _ = interval.tick() => {}
                            _ = notify.notified() => {
                                tracing::debug!("Config reload triggered by local admin write");
                            }
                            rev = recv_revision(&mut revision_rx) => {
                                tracing::debug!(revision = rev, "Config reload triggered by distributed revision change");
                            }
                        }
                        coalesce_revisions(&mut revision_rx).await;
                        do_reload_or_guard(&backend, &cache, &live, &admin_for_reload, &metrics)
                            .await;
                    }
                }
            }
        }
    });

    // Background task: health-check each cluster every 30s via its adapter.
    // Clusters with a custom `healthCheckQuery` use that SQL instead of `SELECT 1`.
    // ADBC SaaS backends without a custom query skip health checks (always healthy).
    tokio::spawn({
        let state = app_state.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let (targets, custom_health) = {
                    let live = state.live.read().await;
                    (
                        live.health_check_targets.clone(),
                        live.custom_health_queries.clone(),
                    )
                };
                for (adapter, cstate) in &targets {
                    let cluster_name = &cstate.cluster_name.0;
                    let healthy = if let Some(custom_sql) = custom_health.get(cluster_name) {
                        adapter.execute_custom_health_check(custom_sql).await
                    } else {
                        adapter.health_check().await
                    };
                    if !healthy {
                        tracing::warn!(
                            cluster = %cluster_name,
                            group = %cstate.group_name.0,
                            "Health check failed — marking cluster unhealthy"
                        );
                    } else if !cstate.is_healthy() {
                        tracing::info!(
                            cluster = %cluster_name,
                            group = %cstate.group_name.0,
                            "Health check recovered — marking cluster healthy"
                        );
                    }
                    cstate.set_healthy(healthy);
                }
            }
        }
    });

    // Background task: reconcile in-memory running_queries counters with ground truth
    // from each engine (engines that implement fetch_running_query_count). Runs every 30s.
    // Corrects drift caused by proxy crashes, client disconnects, or any other leak.
    // In distributed mode, only the replica holding the `engine-reconcile` sweep lock
    // queries backends; it publishes counts to Postgres and other replicas sync locally.
    tokio::spawn({
        let state = app_state.clone();
        let distributed_backend = distributed_backend.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                if !tick_or_shutdown(&mut interval, &mut shutdown_rx).await {
                    break;
                }
                let (targets, custom_reconcile) = {
                    let live = state.live.read().await;
                    (
                        live.health_check_targets.clone(),
                        live.custom_reconcile_queries.clone(),
                    )
                };

                if let Some(ref db) = distributed_backend {
                    let capacity_store =
                        db.clone() as Arc<dyn queryflux_persistence::CapacityStore>;
                    match db.try_sweep_lock("engine-reconcile").await {
                        Ok(Some(lock)) => {
                            for (adapter, cstate) in &targets {
                                let cluster_name = &cstate.cluster_name.0;
                                let actual = fetch_engine_running_count(
                                    adapter,
                                    cluster_name,
                                    &custom_reconcile,
                                )
                                .await;
                                if let Some(count) = actual {
                                    if let Err(e) = capacity_store
                                        .publish_running_count(cluster_name, count)
                                        .await
                                    {
                                        state
                                            .metrics
                                            .on_coordination_failure("engine_reconcile_publish");
                                        tracing::warn!(
                                            cluster = %cluster_name,
                                            "Failed to publish engine running count: {e}"
                                        );
                                    }
                                }
                                apply_reconcile_to_cluster_state(cstate, actual);
                            }
                            let _ = lock.release().await;
                        }
                        Ok(None) => {
                            for (_, cstate) in &targets {
                                let cluster_name = &cstate.cluster_name.0;
                                match capacity_store.active_count(cluster_name).await {
                                    Ok(count) => {
                                        apply_reconcile_to_cluster_state(cstate, Some(count));
                                    }
                                    Err(e) => {
                                        state
                                            .metrics
                                            .on_coordination_failure("engine_reconcile_read");
                                        tracing::warn!(
                                            cluster = %cluster_name,
                                            "Failed to read engine running count: {e}"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            state
                                .metrics
                                .on_coordination_failure("engine_reconcile_sweep_lock");
                            tracing::warn!("Engine reconcile sweep lock failed: {e}");
                        }
                    }
                } else {
                    for (adapter, cstate) in &targets {
                        let cluster_name = &cstate.cluster_name.0;
                        let actual =
                            fetch_engine_running_count(adapter, cluster_name, &custom_reconcile)
                                .await;
                        apply_reconcile_to_cluster_state(cstate, actual);
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
        let state = app_state.clone();
        let rx = shutdown_rx.clone();
        let cfg = trino_cfg;
        async move {
            if cfg.enabled {
                TrinoHttpFrontend::new(state, cfg.port, cfg.max_connections)
                    .listen(rx)
                    .await
            } else {
                std::future::pending::<queryflux_core::error::Result<()>>().await
            }
        }
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

/// Parses the `variants` JSONB column into `ClusterVariant`s. Shared by every
/// startup/reload path that expands a persisted cluster record into runtime
/// clusters, so the parsing behavior (and its `unwrap_or_default` on malformed
/// JSON) stays identical everywhere instead of drifting across call sites.
fn parse_cluster_variants(
    variants_json: &serde_json::Value,
) -> Vec<queryflux_core::config::ClusterVariant> {
    serde_json::from_value(variants_json.clone()).unwrap_or_default()
}

/// Extracts the base `healthCheckQuery` / `reconcileQuery` overrides from a
/// cluster's persisted `config` JSONB blob, ahead of variant expansion (which
/// applies driver defaults and `{{sub_resource}}` substitution on top). Shared
/// by every call site that needs these two fields before calling
/// `expand_cluster_variants`.
fn extract_base_probe_queries(config_json: &serde_json::Value) -> (Option<String>, Option<String>) {
    let health_check_query = config_json
        .get("healthCheckQuery")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let reconcile_query = config_json
        .get("reconcileQuery")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    (health_check_query, reconcile_query)
}

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
    /// Last-successfully-built cluster-selection strategy per group, keyed by group name.
    /// Consulted when a reload's `strategy_from_config` fails (e.g. a `pythonScript` group's
    /// `scriptFile` becomes transiently unreadable) so that group keeps its real strategy
    /// (weighting, engine affinity, …) instead of silently degrading to round robin. Only
    /// updated on a successful build, so it always reflects the last *good* strategy, not
    /// whatever fallback got substituted on a prior failed reload.
    strategies:
        HashMap<String, Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>>,
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
                for rule in rules {
                    if matches!(rule.action, queryflux_core::config::RegexRouteAction::Route) {
                        if let Some(group) = rule.target_group.as_deref() {
                            refs.push(group);
                        } else {
                            issues.push("queryRegex route rule requires targetGroup".to_string());
                        }
                    }
                }
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

/// Per expanded variant: merged JSON config, optional max concurrency, optional probe SQL.
type ExpandedVariantConfig = (
    serde_json::Value,
    Option<u64>,
    Option<String>,
    Option<String>,
);

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
        cluster_state::ClusterState,
        simple::SimpleClusterGroupManager,
        strategy::{strategy_from_config, RoundRobinStrategy},
    };
    use queryflux_core::config::apply_default_probe_queries;
    use queryflux_core::engine_registry::{
        cluster_config_from_persisted_json, json_str, parse_auth_from_config_json,
        parse_engine_key, parse_query_auth_from_config_json,
    };
    use queryflux_core::tags::QueryTags;

    // Build a lookup map from records for group member resolution.
    // This includes both base record names and expanded variant names.
    let records_by_name: HashMap<
        &str,
        &queryflux_persistence::cluster_config::ClusterConfigRecord,
    > = cluster_records
        .iter()
        .map(|r| (r.name.as_str(), r))
        .collect();

    // Expand variants: build a map of all valid cluster names (base + expanded).
    let mut all_cluster_names: HashSet<String> = HashSet::new();
    // Track which expanded names map to which parent record (for records_by_name resolution).
    let mut expanded_to_parent: HashMap<
        String,
        &queryflux_persistence::cluster_config::ClusterConfigRecord,
    > = HashMap::new();
    // Track expanded cluster configs for each variant.
    let mut expanded_configs: HashMap<String, ExpandedVariantConfig> = HashMap::new();

    for record in cluster_records {
        let variants = parse_cluster_variants(&record.variants);

        if variants.is_empty() {
            all_cluster_names.insert(record.name.clone());
        } else {
            let (health_check_query, reconcile_query) = extract_base_probe_queries(&record.config);
            match queryflux_core::config::expand_cluster_variants(
                &record.name,
                &record.config,
                &record.engine_key,
                &variants,
                health_check_query.as_deref(),
                reconcile_query.as_deref(),
            ) {
                Ok(expanded) => {
                    for exp in expanded {
                        all_cluster_names.insert(exp.expanded_name.clone());
                        expanded_to_parent.insert(exp.expanded_name.clone(), record);
                        expanded_configs.insert(
                            exp.expanded_name,
                            (
                                exp.merged_config,
                                exp.max_running_queries,
                                exp.health_check_query,
                                exp.reconcile_query,
                            ),
                        );
                    }
                }
                Err(err) => {
                    tracing::error!(cluster = %record.name, error = %err, "Reload: variant expansion failed — cluster omitted");
                }
            }
        }
    }

    let prev_config_json = cache.config_json.clone();

    // Build adapters — reuse when serialized cluster config is unchanged.
    // First handle base (non-variant) records.
    for record in cluster_records {
        if !parse_cluster_variants(&record.variants).is_empty() {
            continue; // has variants — handled below
        }

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

    // Build adapters for expanded variant clusters.
    for (expanded_name, (merged_config, _, _, _)) in &expanded_configs {
        let parent = match expanded_to_parent.get(expanded_name) {
            Some(p) => p,
            None => continue,
        };
        if !parent.enabled {
            cache.adapters.remove(expanded_name.as_str());
            cache.config_json.remove(expanded_name.as_str());
            continue;
        }
        let cfg_json =
            serde_json::to_string(&(parent.engine_key.as_str(), merged_config)).unwrap_or_default();
        let reuse = cache.adapters.contains_key(expanded_name.as_str())
            && prev_config_json
                .get(expanded_name.as_str())
                .map(String::as_str)
                == Some(cfg_json.as_str());
        if reuse {
            continue;
        }
        cache.adapters.remove(expanded_name.as_str());
        cache.config_json.remove(expanded_name.as_str());

        let cluster_name = ClusterName(expanded_name.clone());
        let placeholder_group = ClusterGroupName("_".to_string());
        let adapter = match registered_engines::build_adapter_from_record(
            cluster_name,
            placeholder_group,
            &parent.engine_key,
            merged_config,
        )
        .await
        {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(
                    cluster = %expanded_name,
                    error = %e,
                    "Reload: failed to build engine adapter for variant — omitted"
                );
                continue;
            }
        };
        cache.adapters.insert(expanded_name.clone(), adapter);
        cache.config_json.insert(expanded_name.clone(), cfg_json);
    }

    cache
        .adapters
        .retain(|name, _| all_cluster_names.contains(name));
    cache
        .config_json
        .retain(|name, _| all_cluster_names.contains(name));

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
            // Resolve member: check base records first, then expanded variant names.
            let record = match records_by_name.get(member_name.as_str()) {
                Some(r) => *r,
                None => match expanded_to_parent.get(member_name.as_str()) {
                    Some(r) => *r,
                    None => {
                        tracing::warn!(group = %group_name, cluster = %member_name, "Reload: group references unknown cluster");
                        continue;
                    }
                },
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
            // For expanded variants, use the variant-specific max_running_queries if set.
            // `?` (not `.unwrap_or_else` — a closure can't propagate `?` out of this
            // function) so an invalid `max_running_queries` on the base record rejects
            // the reload here exactly like the non-variant fallback, instead of
            // silently falling back to the group default.
            let variant_max_override = expanded_configs
                .get(member_name.as_str())
                .and_then(|(_, variant_max, _, _)| *variant_max);
            let max_q = match variant_max_override {
                Some(v) => v,
                None => max_running_queries_u64_from_db(member_name, record.max_running_queries)?
                    .unwrap_or(group_config.max_running_queries),
            };
            // For expanded variants, use merged config for endpoint resolution.
            let effective_config = expanded_configs
                .get(member_name.as_str())
                .map(|(cfg, _, _, _)| cfg)
                .unwrap_or(&record.config);
            let endpoint = json_str(effective_config, "endpoint");
            let cluster_cid = cluster_ids_by_name
                .get(member_name.as_str())
                .copied()
                .or_else(|| expanded_to_parent.get(member_name.as_str()).map(|r| r.id));
            let group_cid = group_ids_by_name.get(group_name.as_str()).copied();

            // When the JSONB + engine_key fingerprint is unchanged, rebuild `ClusterState` from
            // the current record anyway (group membership, IDs, endpoint, max_q may still change)
            // but copy health and queue counters from the previous generation.
            let cfg_json = serde_json::to_string(&(record.engine_key.as_str(), effective_config))
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

        let strategy = match strategy_from_config(group_config.strategy.as_ref()) {
            Ok(s) => {
                cache.strategies.insert(group_name.clone(), s.clone());
                s
            }
            Err(e) => {
                if let Some(prev) = cache.strategies.get(group_name.as_str()) {
                    tracing::warn!(
                        group = %group_name,
                        error = %e,
                        "Reload: failed to build cluster-selection strategy; keeping this group's previous strategy"
                    );
                    prev.clone()
                } else {
                    tracing::warn!(
                        group = %group_name,
                        error = %e,
                        "Reload: failed to build cluster-selection strategy; no previous strategy cached, falling back to round robin"
                    );
                    Arc::new(RoundRobinStrategy::new())
                }
            }
        };
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_order.push(group_name.clone());
        group_states.insert(group_key, (states, strategy));
    }
    group_order.sort();
    // Drop cached strategies for groups that no longer exist, so a deleted-then-recreated
    // group name can't inherit the old group's strategy if the new one's build ever fails.
    cache
        .strategies
        .retain(|name, _| cluster_groups.contains_key(name.as_str()));

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
        let base_cfg = cluster_config_from_persisted_json(
            engine.clone(),
            r.enabled,
            max_running,
            &r.config,
            auth.clone(),
            query_auth.clone(),
        );

        let variants = parse_cluster_variants(&r.variants);
        if variants.is_empty() {
            let mut base_cfg = base_cfg;
            apply_default_probe_queries(&mut base_cfg, &r.engine_key, &r.config);
            cluster_configs.insert(r.name.clone(), base_cfg);
        } else {
            // Insert a ClusterConfig entry for each expanded variant name.
            for (expanded_name, (_, variant_max, hcq, rq)) in &expanded_configs {
                if expanded_name.starts_with(&format!("{}::", r.name)) {
                    let mut variant_cfg = base_cfg.clone();
                    variant_cfg.max_running_queries = variant_max.or(max_running);
                    variant_cfg.health_check_query = hcq.clone();
                    variant_cfg.reconcile_query = rq.clone();
                    cluster_configs.insert(expanded_name.clone(), variant_cfg);
                }
            }
        }
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
            RouterConfig::UserGroup { user_to_group } => {
                let mapping = user_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(
                    queryflux_routing::implementations::user_group::UserGroupRouter::new(mapping),
                ));
            }
            RouterConfig::QueryRegex { rules } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::query_regex::QueryRegexRouter::from_rules(
                        rules.clone(),
                    ),
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
        }
    }
    let router_chain = RouterChain::new(routers, fallback);

    let group_default_tags: HashMap<String, QueryTags> = cluster_groups
        .iter()
        .filter(|(_, g)| !g.default_tags.is_empty())
        .map(|(name, g)| (name.clone(), g.default_tags.clone()))
        .collect();

    let group_max_queued_queries: HashMap<String, Option<u64>> = cluster_groups
        .iter()
        .filter(|(_, g)| g.max_queued_queries.is_some())
        .map(|(name, g)| (name.clone(), g.max_queued_queries))
        .collect();

    let group_capacity_wait_timeout_secs: HashMap<String, u64> = cluster_groups
        .iter()
        .map(|(name, g)| (name.clone(), g.capacity_wait_timeout_secs_or_default()))
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

    // Build custom health/reconcile query maps from cluster configs.
    let mut custom_health_queries: HashMap<String, String> = HashMap::new();
    let mut custom_reconcile_queries: HashMap<String, String> = HashMap::new();
    for (name, cfg) in &cluster_configs {
        if let Some(q) = &cfg.health_check_query {
            custom_health_queries.insert(name.clone(), q.clone());
        }
        if let Some(q) = &cfg.reconcile_query {
            custom_reconcile_queries.insert(name.clone(), q.clone());
        }
    }

    Ok(LiveConfig {
        router_chain,
        guard_chain: None,
        group_guard_chains: HashMap::new(),
        cluster_manager,
        adapters: cache.adapters.clone(),
        health_check_targets,
        custom_health_queries,
        custom_reconcile_queries,
        cluster_configs,
        group_members,
        group_order,
        group_translation_scripts,
        group_default_tags,
        group_max_queued_queries,
        group_capacity_wait_timeout_secs,
        group_cache_settings,
        auth_provider: Arc::new(NoneAuthProvider::new(false)),
        authorization: Arc::new(AllowAllAuthorization::default()),
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
    metrics: &Arc<dyn MetricsStore>,
) -> Result<LiveConfig> {
    let cluster_records = pg
        .list_cluster_configs()
        .await
        .context("reload: list_cluster_configs")?;
    let mut cluster_ids_by_name: HashMap<String, i64> = cluster_records
        .iter()
        .map(|r| (r.name.clone(), r.id))
        .collect();
    // Also map expanded variant names to their parent's DB ID.
    for r in &cluster_records {
        for v in &parse_cluster_variants(&r.variants) {
            cluster_ids_by_name.insert(format!("{}::{}", r.name, v.name), r.id);
        }
    }

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
            metrics.on_config_reload_failure("guard_reload");
            tracing::warn!("Reload: guardrails_config read failed; keeping previous chains: {e}")
        }
    }

    // Rebuild auth/authz from persisted security config. On a missing row or
    // any parse/build failure keep the carried-over providers — a reload must
    // never fall back to permissive defaults.
    match pg.get_proxy_setting("security_config").await {
        Ok(Some(v)) if queryflux_core::security_setting::is_blank_security_setting(&v) => {}
        Ok(Some(v)) => {
            let (auth_cfg, authz_cfg) =
                queryflux_core::security_setting::parse_security_setting(&v);
            match auth_cfg.as_ref().map(|cfg| build_auth_provider(cfg)) {
                Some(Ok(provider)) => live.auth_provider = provider,
                Some(Err(e)) => {
                    metrics.on_config_reload_failure("auth_rebuild");
                    tracing::warn!("Reload: failed to rebuild auth provider; keeping previous: {e}")
                }
                None => {
                    metrics.on_config_reload_failure("auth_rebuild");
                    tracing::warn!(
                        "Reload: security_config has no recognizable auth section; keeping previous"
                    );
                }
            }
            match auth_cfg {
                Some(auth) => {
                    let operators = operators_from_auth(&auth);
                    match authz_cfg.map(|cfg| build_authorization(&cfg, &cluster_groups, operators))
                    {
                        Some(Ok(checker)) => live.authorization = checker,
                        Some(Err(e)) => {
                            metrics.on_config_reload_failure("authz_rebuild");
                            tracing::warn!(
                                "Reload: failed to rebuild authorization; keeping previous: {e}"
                            )
                        }
                        None => {
                            metrics.on_config_reload_failure("authz_rebuild");
                            tracing::warn!(
                            "Reload: security_config has no recognizable authorization section; keeping previous"
                        );
                        }
                    }
                }
                None if authz_cfg.is_some() => {
                    metrics.on_config_reload_failure("authz_rebuild");
                    tracing::warn!(
                        "Reload: security_config has authorization but no auth section; keeping previous authorization (operator policy unchanged)"
                    );
                }
                None => {}
            }
        }
        Ok(None) => {}
        Err(e) => {
            metrics.on_config_reload_failure("auth_rebuild");
            tracing::warn!("Reload: security_config read failed; keeping previous auth: {e}")
        }
    }

    Ok(live)
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

fn operators_from_auth(
    auth: &queryflux_core::config::AuthConfig,
) -> queryflux_auth::OperatorPolicy {
    queryflux_auth::OperatorPolicy::from_lists(
        auth.operator_roles.clone(),
        auth.operator_groups.clone(),
    )
}

fn build_authorization(
    authz: &queryflux_core::config::AuthorizationConfig,
    cluster_groups: &HashMap<String, queryflux_core::config::ClusterGroupConfig>,
    operators: queryflux_auth::OperatorPolicy,
) -> Result<Arc<dyn queryflux_auth::AuthorizationChecker>> {
    use queryflux_core::config::AuthorizationProviderConfig;
    if !operators.roles.is_empty() || !operators.groups.is_empty() {
        info!(
            operator_roles = ?operators.roles,
            operator_groups = ?operators.groups,
            "Query operators configured (may cancel any query)"
        );
    }
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
                Arc::new(SimpleAuthorizationPolicy::new(policies).with_operators(operators))
            } else {
                info!("Authorization: allow-all (no allow-lists configured)");
                Arc::new(AllowAllAuthorization::with_operators(operators))
            }
        }
        AuthorizationProviderConfig::OpenFga => {
            let openfga_cfg = authz.openfga.clone().context(
                "authorization.provider = openfga requires authorization.openfga to be configured",
            )?;
            info!(url = %openfga_cfg.url, store_id = %openfga_cfg.store_id, "Authorization: OpenFGA");
            Arc::new(OpenFgaAuthorizationClient::new(openfga_cfg).with_operators(operators))
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

fn resolve_built_in_guard(
    name: Option<&str>,
    max_rows: Option<u64>,
    applies_to: Option<Vec<String>>,
) -> Box<dyn Guard> {
    match name {
        Some("read_only") => Box::new(ReadOnlyGuard),
        Some("row_limit") => Box::new(RowLimitGuard { max_rows }),
        Some("require_predicate") => Box::new(RequirePredicateGuard {
            applies_to: applies_to.unwrap_or_default(),
        }),
        Some(other) => Box::new(MisconfiguredGuard {
            guard_name: "built_in",
            reason: format!("unsupported built_in guard name \"{other}\""),
        }),
        None => Box::new(MisconfiguredGuard {
            guard_name: "built_in",
            reason: "built_in guard is missing required field \"name\"".to_string(),
        }),
    }
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
    let raw = url.trim();
    if raw.is_empty() {
        tracing::warn!("http_webhook guard has empty URL; using MisconfiguredGuard");
        return Box::new(MisconfiguredGuard {
            guard_name: "http_webhook",
            reason: "http_webhook guard is missing required field \"url\"".to_string(),
        });
    }
    match reqwest::Url::parse(raw) {
        Ok(parsed) => match parsed.scheme() {
            "http" | "https" => Box::new(HttpWebhookGuard {
                url: raw.to_string(),
                timeout_ms,
                retry_count,
                fail_behavior,
                headers,
                client: reqwest::Client::new(),
            }),
            other => {
                tracing::warn!(
                    scheme = other,
                    "http_webhook guard URL must use http or https; using MisconfiguredGuard"
                );
                Box::new(MisconfiguredGuard {
                    guard_name: "http_webhook",
                    reason: format!(
                        "http_webhook url must use http or https scheme, got \"{other}\""
                    ),
                })
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "http_webhook guard URL is invalid; using MisconfiguredGuard");
            Box::new(MisconfiguredGuard {
                guard_name: "http_webhook",
                reason: format!("http_webhook url is not a valid URL: {e}"),
            })
        }
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
                guards.push(resolve_built_in_guard(
                    spec.name.as_deref(),
                    spec.max_rows,
                    spec.applies_to.clone(),
                ));
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
            guards.push(Box::new(MisconfiguredGuard {
                guard_name: "guard",
                reason: "guard spec is not a valid object with a \"kind\" field".to_string(),
            }));
            continue;
        };
        match spec.kind.as_str() {
            "built_in" => {
                guards.push(resolve_built_in_guard(
                    spec.name.as_deref(),
                    spec.max_rows,
                    spec.applies_to,
                ));
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
            other => guards.push(Box::new(MisconfiguredGuard {
                guard_name: "guard",
                reason: format!("unsupported guard kind \"{other}\""),
            })),
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

async fn fetch_engine_running_count(
    adapter: &queryflux_engine_adapters::AdapterKind,
    cluster_name: &str,
    custom_reconcile: &HashMap<String, String>,
) -> Option<u64> {
    if let Some(custom_sql) = custom_reconcile.get(cluster_name) {
        adapter.execute_custom_reconcile_query(custom_sql).await
    } else {
        adapter.fetch_running_query_count().await
    }
}

fn apply_reconcile_to_cluster_state(
    cstate: &queryflux_cluster_manager::cluster_state::ClusterState,
    actual: Option<u64>,
) {
    let cluster_name = &cstate.cluster_name.0;
    let tracked = cstate.running_queries();
    let max = cstate.max_running_queries();
    if tracked > max {
        let fix = actual.unwrap_or(0);
        tracing::warn!(
            cluster = %cluster_name,
            group = %cstate.group_name.0,
            tracked,
            max,
            fix,
            "running_queries above group capacity; resetting from engine count"
        );
        cstate.set_running_queries(fix);
        return;
    }
    if let Some(actual) = actual {
        if actual != tracked {
            tracing::info!(
                cluster = %cluster_name,
                group = %cstate.group_name.0,
                tracked,
                actual,
                "Reconciling running_queries counter with engine ground truth"
            );
            cstate.set_running_queries(actual);
        }
    }
}

/// Metrics chain for in-memory persistence: Prometheus plus the in-memory store
/// itself. The in-memory store must be part of the chain — the admin API serves
/// `/admin/queries` and `/admin/stats` from it, so recording only to Prometheus
/// leaves query history and the Studio dashboard permanently empty.
fn in_memory_metrics(
    prometheus: Arc<PrometheusMetrics>,
    mem: Arc<InMemoryPersistence>,
) -> Arc<dyn MetricsStore> {
    Arc::new(MultiMetricsStore::new(vec![
        prometheus as Arc<dyn MetricsStore>,
        mem as Arc<dyn MetricsStore>,
    ]))
}

#[cfg(test)]
mod tests {
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
        use std::collections::HashMap;

        use queryflux_core::config::{GuardKindConfig, GuardSpecConfig, GuardrailsConfig};
        use queryflux_core::query::{ClusterGroupName, EngineType};
        use queryflux_core::tags::QueryTags;
        use queryflux_guardrails::context::{GuardContext, GuardLayer};

        use super::super::{build_chain_from_db_specs, build_chain_from_yaml_specs};

        fn plan_ctx<'a>(
            engine: &'a EngineType,
            group: &'a ClusterGroupName,
            tags: &'a QueryTags,
        ) -> GuardContext<'a> {
            GuardContext {
                sql: "SELECT 1",
                translated_sql: "SELECT 1",
                engine_type: engine,
                cluster_group: group,
                user: Some("alice"),
                agent_context: None,
                query_tags: tags,
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
            let (actions, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(blocked);
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
            let (_, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(blocked);
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
            let (actions, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(blocked);
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
            let (_, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(!blocked);
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
            let (actions, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(blocked, "non-http(s) webhook URL must deny at construction");
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
            let (_, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(
                !blocked,
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
            let (actions, blocked) = chain.run(&ctx, GuardLayer::Plan).await;
            assert!(blocked);
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
                was_translated: false,
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
            let metrics = super::super::in_memory_metrics(prometheus, mem.clone());

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
