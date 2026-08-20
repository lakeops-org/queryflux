use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use queryflux_core::{config::EngineConfig, query::EngineType};

/// A point-in-time view of one cluster within a group, passed to strategies.
/// Strategies inspect this to pick a candidate — they never mutate state.
pub struct ClusterCandidate<'a> {
    pub name: &'a str,
    pub engine_type: EngineType,
    pub running_queries: u64,
    pub max_running_queries: u64,
}

/// Pluggable cluster selection algorithm.
///
/// Receives a non-empty slice of healthy, enabled, under-capacity candidates
/// and returns the index of the chosen one. Returning `None` from a non-empty
/// slice is treated as "no selection" — the caller falls back to index 0.
pub trait ClusterSelectionStrategy: Send + Sync {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize>;

    /// `true` for strategies whose `pick` may block the calling thread (e.g. embedded
    /// Python holding the GIL). Callers on an async hot path should dispatch these via
    /// `spawn_blocking` instead of calling `pick` inline.
    fn requires_blocking_dispatch(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Round-robin
// ---------------------------------------------------------------------------

pub struct RoundRobinStrategy {
    counter: AtomicU64,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterSelectionStrategy for RoundRobinStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) as usize % candidates.len();
        Some(idx)
    }
}

// ---------------------------------------------------------------------------
// Least loaded (pick cluster with most remaining capacity)
// ---------------------------------------------------------------------------

pub struct LeastLoadedStrategy;

impl ClusterSelectionStrategy for LeastLoadedStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, c)| c.running_queries)
            .map(|(i, _)| i)
    }
}

// ---------------------------------------------------------------------------
// Failover (try clusters in member order)
// ---------------------------------------------------------------------------

pub struct FailoverStrategy;

impl ClusterSelectionStrategy for FailoverStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        // Candidates are already filtered to healthy + under capacity.
        // The first one in the slice is the highest-priority available cluster.
        if candidates.is_empty() {
            None
        } else {
            Some(0)
        }
    }
}

// ---------------------------------------------------------------------------
// Engine affinity (prefer engines in a given order for mixed-engine groups)
// ---------------------------------------------------------------------------

pub struct EngineAffinityStrategy {
    preference: Vec<EngineType>,
}

impl EngineAffinityStrategy {
    pub fn new(preference: Vec<EngineConfig>) -> Self {
        Self {
            preference: preference.iter().map(EngineType::from).collect(),
        }
    }
}

impl ClusterSelectionStrategy for EngineAffinityStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        // Find the highest-priority engine type that has at least one candidate.
        for preferred_engine in &self.preference {
            let engine_candidates: Vec<usize> = candidates
                .iter()
                .enumerate()
                .filter(|(_, c)| &c.engine_type == preferred_engine)
                .map(|(i, _)| i)
                .collect();
            if !engine_candidates.is_empty() {
                // Among candidates of the preferred engine, pick the least loaded.
                return engine_candidates
                    .into_iter()
                    .min_by_key(|&i| candidates[i].running_queries);
            }
        }
        // No preferred engine available — fall back to first candidate.
        Some(0)
    }
}

// ---------------------------------------------------------------------------
// Weighted random
// ---------------------------------------------------------------------------

pub struct WeightedStrategy {
    /// Ordered list of (cluster_name, weight) matching the group's member list order.
    weights: Vec<(String, u32)>,
}

impl WeightedStrategy {
    pub fn new(weights: HashMap<String, u32>) -> Self {
        Self {
            weights: weights.into_iter().collect(),
        }
    }
}

impl ClusterSelectionStrategy for WeightedStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }

        // Build (candidate_index, weight) pairs for eligible candidates.
        let weighted: Vec<(usize, u32)> = candidates
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = self
                    .weights
                    .iter()
                    .find(|(name, _)| name == c.name)
                    .map(|(_, w)| *w)
                    .unwrap_or(1);
                (i, w)
            })
            .collect();

        let total: u32 = weighted.iter().map(|(_, w)| w).sum();
        if total == 0 {
            return Some(0);
        }

        // Deterministic pseudo-random using sum of running queries as seed.
        // Good enough for load distribution without needing an RNG dependency.
        let seed: u64 = candidates
            .iter()
            .map(|c| c.running_queries)
            .sum::<u64>()
            .wrapping_add(candidates.len() as u64)
            .wrapping_mul(2654435761);
        let roll = (seed % total as u64) as u32;

        let mut acc = 0u32;
        for (idx, weight) in &weighted {
            acc += weight;
            if roll < acc {
                return Some(*idx);
            }
        }
        weighted.last().map(|(i, _)| *i)
    }
}

// ---------------------------------------------------------------------------
// Python script (operator-supplied cluster selection)
// ---------------------------------------------------------------------------

/// Runs an operator-supplied `def select_cluster(candidates: list[dict]) -> str | None`
/// to pick a member cluster when no built-in strategy fits. Mirrors
/// `queryflux_routing::implementations::python_script::PythonScriptRouter`, one level
/// down: that picks a cluster *group*, this picks a *member* within an already-chosen group.
pub struct PythonScriptStrategy {
    script: String,
    /// Compiled once, on first `pick()`, then reused for the lifetime of this strategy
    /// instance (one instance per config generation — see `strategy_from_config`) so a
    /// hot group doesn't re-parse and re-execute the script's top level on every query.
    /// `Err` is cached too: a script that fails to compile stays failed until the next
    /// reload builds a fresh instance, rather than re-attempting compilation every pick.
    compiled: std::sync::OnceLock<Result<pyo3::Py<pyo3::PyAny>, String>>,
}

impl PythonScriptStrategy {
    pub fn new(script: String) -> Self {
        Self {
            script,
            compiled: std::sync::OnceLock::new(),
        }
    }

    /// Load from a file path instead of inline script.
    pub fn from_file(path: &str) -> queryflux_core::error::Result<Self> {
        let script = std::fs::read_to_string(path).map_err(|e| {
            queryflux_core::error::QueryFluxError::Config(format!(
                "Failed to read cluster-selection script {path}: {e}"
            ))
        })?;
        Ok(Self::new(script))
    }

    /// Compiles `self.script` and looks up `select_cluster` the first time this is
    /// called; every later call reuses the cached function object.
    fn compiled_select_fn(&self) -> &Result<pyo3::Py<pyo3::PyAny>, String> {
        use pyo3::types::{PyDict, PyDictMethods};
        use pyo3::Python;

        self.compiled.get_or_init(|| {
            Python::attach(|py| {
                let globals = PyDict::new(py);
                let cscript = std::ffi::CString::new(self.script.as_str())
                    .map_err(|e| format!("script contains null byte: {e}"))?;
                py.run(&cscript, Some(&globals), None)
                    .map_err(|e| format!("python cluster-selection script error: {e}"))?;

                let select_fn = globals
                    .get_item("select_cluster")
                    .map_err(|e| format!("script has no 'select_cluster' function: {e}"))?
                    .ok_or_else(|| "script has no 'select_cluster' function".to_string())?;

                Ok(select_fn.unbind())
            })
        })
    }
}

impl ClusterSelectionStrategy for PythonScriptStrategy {
    fn requires_blocking_dispatch(&self) -> bool {
        true
    }

    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        match call_python_select(self.compiled_select_fn(), candidates) {
            Ok(picked) => picked,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "python cluster-selection script failed; falling back to first candidate"
                );
                None
            }
        }
    }
}

fn call_python_select(
    compiled: &Result<pyo3::Py<pyo3::PyAny>, String>,
    candidates: &[ClusterCandidate<'_>],
) -> Result<Option<usize>, String> {
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods};
    use pyo3::Python;

    let select_fn = compiled.as_ref().map_err(|e| e.clone())?;

    Python::attach(|py| {
        let select_fn = select_fn.bind(py);

        let candidate_list = PyList::empty(py);
        for c in candidates {
            let d = PyDict::new(py);
            let engine_type = serde_json::to_value(&c.engine_type)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            d.set_item("name", c.name)
                .and_then(|_| d.set_item("engineType", engine_type))
                .and_then(|_| d.set_item("runningQueries", c.running_queries))
                .and_then(|_| d.set_item("maxRunningQueries", c.max_running_queries))
                .map_err(|e| format!("failed to build candidate dict: {e}"))?;
            candidate_list
                .append(d)
                .map_err(|e| format!("failed to build candidate list: {e}"))?;
        }

        let result = select_fn.call1((candidate_list,)).map_err(|e| {
            format!(
                "select_cluster(candidates) call failed: {e} \
                 (expected def select_cluster(candidates: list[dict]) -> str | None)"
            )
        })?;

        if result.is_none() {
            return Ok(None);
        }

        let name: String = result
            .extract()
            .map_err(|e| format!("select_cluster() must return str or None: {e}"))?;

        let picked = candidates.iter().position(|c| c.name == name);
        if picked.is_none() {
            tracing::warn!(
                returned = %name,
                "python cluster-selection script returned a name not in the eligible candidate \
                 list; falling back to first candidate"
            );
        }
        Ok(picked)
    })
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub fn strategy_from_config(
    config: Option<&queryflux_core::config::StrategyConfig>,
) -> queryflux_core::error::Result<Arc<dyn ClusterSelectionStrategy>> {
    use queryflux_core::config::StrategyConfig;
    use queryflux_core::error::QueryFluxError;
    Ok(match config {
        None | Some(StrategyConfig::RoundRobin) => Arc::new(RoundRobinStrategy::new()),
        Some(StrategyConfig::LeastLoaded) => Arc::new(LeastLoadedStrategy),
        Some(StrategyConfig::Failover) => Arc::new(FailoverStrategy),
        Some(StrategyConfig::EngineAffinity { preference }) => {
            Arc::new(EngineAffinityStrategy::new(preference.clone()))
        }
        Some(StrategyConfig::Weighted { weights }) => {
            Arc::new(WeightedStrategy::new(weights.clone()))
        }
        // `scriptFile` wins when both are set — same precedence as `RouterConfig::PythonScript`
        // in `crates/queryflux/src/main.rs`.
        Some(StrategyConfig::PythonScript {
            script,
            script_file,
        }) => match script_file {
            Some(path) => Arc::new(PythonScriptStrategy::from_file(path)?),
            None => match script {
                Some(inline) => Arc::new(PythonScriptStrategy::new(inline.clone())),
                None => {
                    return Err(QueryFluxError::Config(
                        "pythonScript strategy has neither 'script' nor 'scriptFile'".to_string(),
                    ))
                }
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::query::EngineType;

    fn candidate<'a>(name: &'a str, running: u64, max: u64) -> ClusterCandidate<'a> {
        ClusterCandidate {
            name,
            engine_type: EngineType::Trino,
            running_queries: running,
            max_running_queries: max,
        }
    }

    #[test]
    fn python_script_picks_named_candidate() {
        let script = r#"
def select_cluster(candidates):
    for c in candidates:
        if c["runningQueries"] == 0:
            return c["name"]
    return None
"#;
        let strategy = PythonScriptStrategy::new(script.to_string());
        let candidates = vec![candidate("a", 5, 10), candidate("b", 0, 10)];
        assert_eq!(strategy.pick(&candidates), Some(1));
    }

    #[test]
    fn python_script_is_compiled_once_and_module_state_persists_across_picks() {
        // If the script were re-parsed and re-executed on every pick(), `_state` would
        // reset to {"count": 0} each time and select_cluster would always return
        // "call-1". Seeing the counter advance proves the compiled module (and its
        // globals) is reused across calls rather than rebuilt from source each time.
        let script = r#"
_state = {"count": 0}

def select_cluster(candidates):
    _state["count"] += 1
    return "call-" + str(_state["count"])
"#;
        let strategy = PythonScriptStrategy::new(script.to_string());
        let candidates = vec![
            candidate("call-1", 0, 10),
            candidate("call-2", 0, 10),
            candidate("call-3", 0, 10),
        ];
        assert_eq!(strategy.pick(&candidates), Some(0));
        assert_eq!(strategy.pick(&candidates), Some(1));
        assert_eq!(strategy.pick(&candidates), Some(2));
    }

    #[test]
    fn python_script_none_falls_back_to_default() {
        let script = "def select_cluster(candidates):\n    return None\n";
        let strategy = PythonScriptStrategy::new(script.to_string());
        let candidates = vec![candidate("a", 0, 10)];
        assert_eq!(strategy.pick(&candidates), None);
    }

    #[test]
    fn python_script_unknown_name_falls_back_to_default() {
        let script = "def select_cluster(candidates):\n    return 'does-not-exist'\n";
        let strategy = PythonScriptStrategy::new(script.to_string());
        let candidates = vec![candidate("a", 0, 10)];
        assert_eq!(strategy.pick(&candidates), None);
    }

    #[test]
    fn python_script_error_falls_back_to_default() {
        let strategy = PythonScriptStrategy::new("this is not valid python".to_string());
        let candidates = vec![candidate("a", 0, 10)];
        assert_eq!(strategy.pick(&candidates), None);
    }

    #[test]
    fn python_script_requires_blocking_dispatch() {
        let strategy = PythonScriptStrategy::new(String::new());
        assert!(strategy.requires_blocking_dispatch());
        assert!(!RoundRobinStrategy::new().requires_blocking_dispatch());
    }

    #[test]
    fn factory_builds_python_script_strategy_from_inline() {
        use queryflux_core::config::StrategyConfig;
        let cfg = StrategyConfig::PythonScript {
            script: Some(
                "def select_cluster(candidates):\n    return candidates[0][\"name\"]\n".to_string(),
            ),
            script_file: None,
        };
        let strategy = strategy_from_config(Some(&cfg)).unwrap();
        let candidates = vec![candidate("only", 0, 10)];
        assert_eq!(strategy.pick(&candidates), Some(0));
    }

    #[test]
    fn factory_rejects_python_script_with_neither_script_nor_file() {
        use queryflux_core::config::StrategyConfig;
        let cfg = StrategyConfig::PythonScript {
            script: None,
            script_file: None,
        };
        let err = strategy_from_config(Some(&cfg)).err().unwrap();
        assert!(err
            .to_string()
            .contains("neither 'script' nor 'scriptFile'"));
    }

    #[test]
    fn factory_prefers_script_file_over_inline_script_when_both_set() {
        use queryflux_core::config::StrategyConfig;
        let dir = std::env::temp_dir();
        let path = dir.join(format!("queryflux-strategy-test-{}.py", std::process::id()));
        std::fs::write(
            &path,
            "def select_cluster(candidates):\n    return 'from-file'\n",
        )
        .unwrap();

        let cfg = StrategyConfig::PythonScript {
            script: Some("def select_cluster(candidates):\n    return 'from-inline'\n".to_string()),
            script_file: Some(path.to_string_lossy().to_string()),
        };
        let strategy = strategy_from_config(Some(&cfg)).unwrap();
        let candidates = vec![
            candidate("from-inline", 0, 10),
            candidate("from-file", 0, 10),
        ];
        assert_eq!(
            strategy.pick(&candidates),
            Some(1),
            "scriptFile should win over inline script"
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn factory_propagates_missing_script_file_error() {
        use queryflux_core::config::StrategyConfig;
        let cfg = StrategyConfig::PythonScript {
            script: None,
            script_file: Some("/does/not/exist/select.py".to_string()),
        };
        assert!(strategy_from_config(Some(&cfg)).is_err());
    }
}
