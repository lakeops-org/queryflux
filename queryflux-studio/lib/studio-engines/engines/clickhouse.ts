import type { FlatClusterForm } from "@/lib/cluster-form/types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

function validateClickHouseClusterFlat(flat: FlatClusterForm): string[] {
  const raw = flat.maxResultBufferBytes?.trim();
  if (raw) {
    const n = Number(raw);
    if (!Number.isInteger(n) || n < 1) {
      return ["ClickHouse: max result buffer must be a positive integer (bytes)."];
    }
  }
  return [];
}

export const clickHouseStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "clickHouse",
    displayName: "ClickHouse",
    description:
      "Real-time OLAP database. Connects via the ClickHouse HTTP interface.",
    hex: "FFCC01",
    connectionType: "http",
    defaultPort: 8123,
    endpointExample: "http://clickhouse:8123",
    supportedAuth: ["basic"],
    implemented: true,
    configFields: [
      {
        key: "endpoint",
        label: "Endpoint",
        description: "HTTP base URL of the ClickHouse server.",
        fieldType: "url",
        required: true,
        example: "http://clickhouse:8123",
      },
      {
        key: "auth.type",
        label: "Auth type",
        description: "Must be 'basic' for ClickHouse (username + password).",
        fieldType: "text",
        required: false,
        example: "basic",
      },
      {
        key: "auth.username",
        label: "Username",
        description: "ClickHouse username.",
        fieldType: "text",
        required: false,
        example: "default",
      },
      {
        key: "auth.password",
        label: "Password",
        description: "ClickHouse password.",
        fieldType: "secret",
        required: false,
      },
      {
        key: "maxResultBufferBytes",
        label: "Max result buffer (bytes)",
        description:
          "Per-query cap on the result bytes QueryFlux buffers in memory. Defaults to 1 GiB when omitted.",
        fieldType: "number",
        required: false,
        example: "1073741824",
      },
      {
        key: "tls.insecureSkipVerify",
        label: "Skip TLS verification",
        description:
          "Disable TLS certificate verification. Use only in development.",
        fieldType: "boolean",
        required: false,
        example: "false",
      },
    ],
  },
  catalog: {
    category: "Open Source OLAP",
    simpleIconSlug: "siClickhouse",
    catalogDescription: "Real-time OLAP database management system",
  },
  validateFlat: validateClickHouseClusterFlat,
};
