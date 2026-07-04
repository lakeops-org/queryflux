use async_trait::async_trait;

/// Non-pool introspection for ADBC drivers that must not use default SQL health
/// checks or reconcile queries (Databricks REST, auto-suspending SaaS warehouses).
#[async_trait]
pub trait AdbcIntrospection: Send + Sync {
    async fn health_check(&self) -> bool;
    async fn fetch_running_query_count(&self) -> Option<u64>;
}
