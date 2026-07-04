/**
 * Default health/reconcile SQL templates shown in Studio when the operator has
 * not set custom queries. QueryFlux applies the same defaults at runtime when
 * these fields are omitted from persisted config.
 */

export function defaultHealthCheckQuery(driver: string): string | undefined {
  switch (driver) {
    case "snowflake":
      return "SHOW WAREHOUSES LIKE '{{sub_resource}}'";
    default:
      return undefined;
  }
}

export function defaultReconcileQuery(driver: string): string | undefined {
  switch (driver) {
    case "snowflake":
      return "SHOW WAREHOUSES LIKE '{{sub_resource}}'";
    case "bigquery":
      return "SELECT COUNT(*) FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE state = 'RUNNING' AND project_id = '{{sub_resource}}'";
    case "redshift":
      return "SELECT COUNT(*) FROM stv_recents WHERE status = 'Running'";
    case "trino":
      return "SELECT count(*) - 1 FROM system.runtime.queries WHERE state = 'RUNNING'";
    case "flightsql":
      return "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND = 'Query'";
    case "clickhouse":
      return "SELECT count() FROM system.processes";
    default:
      return undefined;
  }
}

export function reconcileQueryPlaceholder(driver: string, saasDriver: boolean): string {
  return (
    defaultReconcileQuery(driver) ??
    (saasDriver
      ? driver === "databricks"
        ? "Built-in REST reconcile (leave empty)"
        : "Leave empty for built-in driver reconcile"
      : "Leave empty to use built-in reconcile SQL")
  );
}

export function healthCheckQueryPlaceholder(driver: string, saasDriver: boolean): string {
  return (
    defaultHealthCheckQuery(driver) ??
    (saasDriver
      ? driver === "databricks"
        ? "Built-in REST health check (leave empty)"
        : "Leave empty for built-in driver introspection"
      : "Leave empty to skip health checks")
  );
}
