import type { ClusterVariant } from "@/lib/api-types";

/** ADBC SaaS drivers that expand into sub-resource variants (warehouses, projects, …). */
export const SAAS_VARIANT_DRIVERS = [
  "snowflake",
  "databricks",
  "bigquery",
  "redshift",
] as const;

export type SaasVariantDriver = (typeof SAAS_VARIANT_DRIVERS)[number];

export function isSaasVariantDriver(driver: string): driver is SaasVariantDriver {
  return (SAAS_VARIANT_DRIVERS as readonly string[]).includes(driver);
}

const VARIANT_NAME_RE = /^[a-zA-Z][a-zA-Z0-9_-]{0,62}$/;

export interface VariantRow {
  /** Stable React key — not persisted. */
  id: string;
  name: string;
  subResource: string;
  maxRunningQueries: string;
}

export interface SubResourceFieldSpec {
  overrideKey: string;
  label: string;
  placeholder: string;
  required: boolean;
}

export function subResourceFieldSpec(driver: string): SubResourceFieldSpec | null {
  switch (driver) {
    case "snowflake":
      return {
        overrideKey: "warehouse",
        label: "Warehouse",
        placeholder: "ANALYTICS_WH",
        required: true,
      };
    case "databricks":
      return {
        overrideKey: "httpPath",
        label: "HTTP path",
        placeholder: "/sql/1.0/warehouses/abc123def456",
        required: true,
      };
    case "bigquery":
      return {
        overrideKey: "project",
        label: "GCP project",
        placeholder: "my-gcp-project",
        required: true,
      };
    case "redshift":
      return null;
    default:
      return null;
  }
}

export function saasVariantsSectionTitle(driver: string): string {
  switch (driver) {
    case "snowflake":
      return "Warehouses";
    case "databricks":
      return "SQL warehouses";
    case "bigquery":
      return "Projects";
    default:
      return "Variants";
  }
}

export function newVariantRow(): VariantRow {
  return {
    id: crypto.randomUUID(),
    name: "",
    subResource: "",
    maxRunningQueries: "",
  };
}

export function variantsToRows(
  variants: ClusterVariant[] | undefined,
  driver: string,
): VariantRow[] {
  const spec = subResourceFieldSpec(driver);
  if (!variants?.length) return [];
  return variants.map((v) => ({
    id: crypto.randomUUID(),
    name: v.name ?? "",
    subResource:
      spec && v.overrides && typeof v.overrides[spec.overrideKey] === "string"
        ? (v.overrides[spec.overrideKey] as string)
        : "",
    maxRunningQueries:
      v.maxRunningQueries != null && v.maxRunningQueries !== undefined
        ? String(v.maxRunningQueries)
        : "",
  }));
}

export function rowsToVariants(rows: VariantRow[], driver: string): ClusterVariant[] {
  const spec = subResourceFieldSpec(driver);
  return rows
    .filter((r) => r.name.trim())
    .map((r) => {
      const variant: ClusterVariant = { name: r.name.trim() };
      if (spec && r.subResource.trim()) {
        variant.overrides = { [spec.overrideKey]: r.subResource.trim() };
      }
      const maxTrim = r.maxRunningQueries.trim();
      if (maxTrim !== "") {
        variant.maxRunningQueries = Number.parseInt(maxTrim, 10);
      }
      return variant;
    });
}

export function validateVariantRows(rows: VariantRow[], driver: string): string[] {
  const errors: string[] = [];
  const spec = subResourceFieldSpec(driver);
  const seen = new Set<string>();

  for (const row of rows) {
    const name = row.name.trim();
    if (!name) continue;

    if (!VARIANT_NAME_RE.test(name)) {
      errors.push(
        `Variant "${name}": use letters, numbers, _ or - (must start with a letter).`,
      );
    }
    if (name.includes("::")) {
      errors.push(`Variant "${name}": name must not contain ::.`);
    }
    if (seen.has(name)) {
      errors.push(`Duplicate variant name "${name}".`);
    }
    seen.add(name);

    if (spec?.required && !row.subResource.trim()) {
      errors.push(`Variant "${name}": ${spec.label.toLowerCase()} is required.`);
    }

    const maxTrim = row.maxRunningQueries.trim();
    if (maxTrim !== "") {
      const n = Number.parseInt(maxTrim, 10);
      if (!Number.isFinite(n) || n < 1 || String(n) !== maxTrim) {
        errors.push(`Variant "${name}": max concurrent queries must be a positive integer.`);
      }
    }
  }

  return errors;
}

export function expandedClusterNames(baseName: string, rows: VariantRow[]): string[] {
  return rows
    .filter((r) => r.name.trim())
    .map((r) => `${baseName}::${r.name.trim()}`);
}
