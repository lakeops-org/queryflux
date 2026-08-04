use clap::Parser;
use queryflux_config::{yaml::YamlFileConfigProvider, ConfigProvider};
use queryflux_core::config::{EngineConfig, ProxyConfig, RouterConfig};
use queryflux_core::query::{EngineType, FrontendProtocol, SqlDialect};

#[derive(Parser, Debug)]
#[command(name = "queryflux", about = "Multi-engine SQL query proxy", version)]
pub struct Cli {
    #[arg(short, long, default_value = "config.yaml")]
    pub config: String,

    /// Validate config and runtime environment (Python/sqlglot) and exit
    #[arg(short, long)]
    pub validate: bool,

    /// Install required Python dependencies (sqlglot) in a local .venv and exit
    #[arg(long)]
    pub install_deps: bool,
}

/// Automate Python path setup if .venv exists and PYTHONPATH/PYO3_PYTHON are unset
fn setup_env() {
    if std::env::var("PYO3_PYTHON").is_err() || std::env::var("PYTHONPATH").is_err() {
        let cwd = std::env::current_dir().unwrap_or_default();
        let venv_dir = cwd.join(".venv");
        if venv_dir.exists() {
            if std::env::var("PYO3_PYTHON").is_err() {
                let venv_paths = vec![
                    venv_dir.join("bin/python3"),
                    venv_dir.join("bin/python"),
                    venv_dir.join("Scripts/python.exe"),
                ];
                for path in venv_paths {
                    if path.exists() {
                        if let Some(path_str) = path.to_str() {
                            std::env::set_var("PYO3_PYTHON", path_str);
                            break;
                        }
                    }
                }
            }

            // Set PYTHONPATH by finding .venv/lib/python*/site-packages or .venv/Lib/site-packages
            if std::env::var("PYTHONPATH").is_err() {
                let mut site_pkg_path = None;

                // Windows style: .venv/Lib/site-packages
                let win_site = venv_dir.join("Lib").join("site-packages");
                if win_site.exists() {
                    site_pkg_path = Some(win_site);
                } else {
                    // POSIX style: .venv/lib/python*/site-packages
                    let lib_dir = venv_dir.join("lib");
                    if lib_dir.exists() {
                        if let Ok(entries) = std::fs::read_dir(lib_dir) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_dir() {
                                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                        if name.starts_with("python") {
                                            let site_packages = path.join("site-packages");
                                            if site_packages.exists() {
                                                site_pkg_path = Some(site_packages);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(path) = site_pkg_path {
                    if let Some(site_str) = path.to_str() {
                        std::env::set_var("PYTHONPATH", site_str);
                    }
                }
            }
        }
    }
}

/// Create a local Python virtual environment and install dependencies
fn handle_install_deps() -> std::io::Result<()> {
    println!("Creating Python virtual environment in .venv...");

    // Find python executable to use for venv creation
    let py_cmds = if let Ok(pyo3_py) = std::env::var("PYO3_PYTHON") {
        vec![pyo3_py]
    } else {
        vec!["python3".to_string(), "python".to_string()]
    };

    let mut venv_status = None;
    for cmd in py_cmds {
        if let Ok(status) = std::process::Command::new(&cmd)
            .args(["-m", "venv", ".venv"])
            .status()
        {
            venv_status = Some(status);
            if status.success() {
                break;
            }
        }
    }

    match venv_status {
        Some(status) if status.success() => {
            println!("✓ Virtual environment created successfully.");
        }
        _ => {
            return Err(std::io::Error::other(
                "Failed to create virtual environment. Ensure python3/python and python3-venv are installed.",
            ));
        }
    }

    let cwd = std::env::current_dir().unwrap_or_default();
    let pip_path = if cfg!(windows) {
        cwd.join(".venv/Scripts/pip.exe")
    } else {
        cwd.join(".venv/bin/pip")
    };

    let mut pip_cmd = std::process::Command::new(&pip_path);
    pip_cmd.arg("install");

    let req_path = cwd.join("requirements.txt");
    if req_path.exists() {
        println!(
            "Installing Python packages from requirements.txt inside the virtual environment..."
        );
        pip_cmd.args(["-r", "requirements.txt"]);
    } else {
        println!("Installing 'sqlglot' package inside the virtual environment...");
        pip_cmd.arg("sqlglot");
    }

    let pip_status = pip_cmd.status()?;
    if pip_status.success() {
        println!("✓ Dependencies installed successfully.");
        Ok(())
    } else {
        Err(std::io::Error::other(
            "Failed to install dependencies. Make sure you have internet access.",
        ))
    }
}

fn config_requires_translation(config: &ProxyConfig) -> bool {
    if !config.translation.python_scripts.is_empty() {
        return true;
    }

    let mut target_groups = vec![config.routing_fallback.clone()];

    for router in &config.routers {
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
                if let Some(g) = trino_http {
                    target_groups.push(g.clone());
                }
                if let Some(g) = postgres_wire {
                    target_groups.push(g.clone());
                }
                if let Some(g) = mysql_wire {
                    target_groups.push(g.clone());
                }
                if let Some(g) = clickhouse_http {
                    target_groups.push(g.clone());
                }
                if let Some(g) = flight_sql {
                    target_groups.push(g.clone());
                }
                if let Some(g) = snowflake_http {
                    target_groups.push(g.clone());
                }
                if let Some(g) = snowflake_sql_api {
                    target_groups.push(g.clone());
                }
            }
            RouterConfig::Header {
                header_value_to_group,
                ..
            } => {
                for g in header_value_to_group.values() {
                    target_groups.push(g.clone());
                }
            }
            RouterConfig::UserGroup { user_to_group, .. } => {
                for g in user_to_group.values() {
                    target_groups.push(g.clone());
                }
            }
            RouterConfig::QueryRegex { rules } => {
                for rule in rules {
                    target_groups.push(rule.target_group.clone());
                }
            }
            RouterConfig::Tags { rules } => {
                for rule in rules {
                    target_groups.push(rule.target_group.clone());
                }
            }
            RouterConfig::Compound { target_group, .. } => {
                target_groups.push(target_group.clone());
            }
            RouterConfig::PythonScript { .. } => {
                return true;
            }
        }
    }

    let mut enabled_frontends = Vec::new();
    if config.queryflux.frontends.trino_http.enabled {
        enabled_frontends.push((FrontendProtocol::TrinoHttp, SqlDialect::Trino));
    }
    if let Some(ref f) = config.queryflux.frontends.postgres_wire {
        if f.enabled {
            enabled_frontends.push((FrontendProtocol::PostgresWire, SqlDialect::Postgres));
        }
    }
    if let Some(ref f) = config.queryflux.frontends.mysql_wire {
        if f.enabled {
            enabled_frontends.push((FrontendProtocol::MySqlWire, SqlDialect::MySql));
        }
    }
    if let Some(ref f) = config.queryflux.frontends.clickhouse_http {
        if f.enabled {
            enabled_frontends.push((FrontendProtocol::ClickHouseHttp, SqlDialect::ClickHouse));
        }
    }
    if let Some(ref f) = config.queryflux.frontends.flight_sql {
        if f.enabled {
            enabled_frontends.push((FrontendProtocol::FlightSql, SqlDialect::Generic));
        }
    }
    if let Some(ref f) = config.queryflux.frontends.snowflake_http {
        if f.enabled {
            enabled_frontends.push((FrontendProtocol::SnowflakeHttp, SqlDialect::Snowflake));
        }
    }

    if enabled_frontends.is_empty() {
        return false;
    }

    for (_, proto_dialect) in enabled_frontends {
        for group_name in &target_groups {
            if let Some(group) = config.cluster_groups.get(group_name) {
                for member in &group.members {
                    if let Some(cluster) = config.clusters.get(member) {
                        if let Some(ref engine) = cluster.engine {
                            let engine_type = match engine {
                                EngineConfig::Trino => EngineType::Trino,
                                EngineConfig::DuckDb => EngineType::DuckDb,
                                EngineConfig::DuckDbHttp => EngineType::DuckDbHttp,
                                EngineConfig::StarRocks => EngineType::StarRocks,
                                EngineConfig::ClickHouse => EngineType::ClickHouse,
                                EngineConfig::Athena => EngineType::Athena,
                                EngineConfig::Adbc => EngineType::Adbc,
                            };
                            if engine_type.dialect() != proto_dialect {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

async fn handle_validation(config_path: &str) {
    println!("Validating configuration file: {}", config_path);
    let config_res = YamlFileConfigProvider::new(config_path).load().await;
    let config = match config_res {
        Ok(c) => {
            println!("✓ Configuration file is valid YAML and matches the schema.");
            c
        }
        Err(e) => {
            eprintln!("✗ Failed to load or parse configuration file: {:?}", e);
            std::process::exit(1);
        }
    };

    let requires_translation = config_requires_translation(&config);

    println!("Validating Python & sqlglot environment...");
    match queryflux_translation::TranslationService::new_sqlglot(
        config.translation.python_scripts.clone(),
    ) {
        Ok(_) => {
            println!("✓ Python interpreter and 'sqlglot' library are available and correctly configured.");
        }
        Err(e) => {
            if requires_translation {
                eprintln!("✗ Python/sqlglot dependency check failed: {}", e);
                println!("\nDialect translation is required by your configuration.");

                use std::io::{self, IsTerminal, Write};
                print!("Would you like to automatically create a virtual environment (.venv) and install 'sqlglot'? [y/N]: ");
                let _ = io::stdout().flush();
                let mut response = String::new();
                let mut confirmed = false;
                if io::stdin().is_terminal() && io::stdin().read_line(&mut response).is_ok() {
                    let trimmed = response.trim().to_lowercase();
                    if trimmed == "y" || trimmed == "yes" {
                        confirmed = true;
                    }
                }

                if confirmed {
                    println!();
                    if let Err(err) = handle_install_deps() {
                        eprintln!("✗ {}", err);
                        std::process::exit(1);
                    }

                    // Re-setup env variables and re-check sqlglot
                    setup_env();
                    if let Err(e2) = queryflux_translation::TranslationService::new_sqlglot(
                        config.translation.python_scripts.clone(),
                    ) {
                        eprintln!(
                            "✗ SQL translation check failed even after installation: {}",
                            e2
                        );
                        std::process::exit(1);
                    }
                    println!("✓ Python interpreter and 'sqlglot' library successfully configured after auto-installation.");
                } else {
                    eprintln!("\nTo run QueryFlux with SQL translation, please ensure that:");
                    eprintln!("  1. Python 3.10+ is installed on the host.");
                    eprintln!("  2. 'sqlglot' package is installed in your Python environment (run: pip install sqlglot).");
                    eprintln!("  3. If using a virtual environment (venv), activate it or run 'queryflux --install-deps'.");
                    std::process::exit(1);
                }
            } else {
                println!("✓ Python/sqlglot is not available ({}), but translation is not required for this configuration.", e);
            }
        }
    }

    println!("\n✓ Validation successful! Configuration and runtime dependencies are correctly configured.");
    std::process::exit(0);
}

/// The main entry point for the CLI. Returns the config path.
pub async fn run_cli() -> String {
    let cli = Cli::parse();

    setup_env();

    if cli.install_deps {
        if let Err(e) = handle_install_deps() {
            eprintln!("✗ {}", e);
            std::process::exit(1);
        }
        println!("\n✓ Dependency installation successful! You can now run QueryFlux.");
        std::process::exit(0);
    }

    if cli.validate {
        handle_validation(&cli.config).await;
    }

    cli.config
}
