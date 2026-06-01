#!/usr/bin/env bash
# Validate the QueryFlux Helm chart structure and render it with helm.
#
# Replaces the former Ruby implementation so the repo carries no Ruby
# dependency. Needs only `helm` (and `python3`, already used by `make setup`,
# for JSON validation). `helm lint` validates Chart.yaml and values against
# values.schema.json; `helm template` renders the default values and every
# file under examples/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="$ROOT/charts/queryflux"

fail() {
  echo "helm chart check failed: $1" >&2
  exit 1
}

command -v helm >/dev/null 2>&1 || fail "helm not found on PATH (install: https://helm.sh/docs/intro/install/)"

required_files=(
  "Chart.yaml"
  "README.md"
  "examples/external-config-values.yaml"
  "examples/production-values.yaml"
  "values.yaml"
  "values.schema.json"
  "templates/_helpers.tpl"
  "templates/deployment.yaml"
  "templates/service.yaml"
  "templates/configmap.yaml"
  "templates/secret.yaml"
  "templates/serviceaccount.yaml"
  "templates/ingress.yaml"
  "templates/hpa.yaml"
  "templates/pdb.yaml"
  "templates/networkpolicy.yaml"
  "templates/servicemonitor.yaml"
  "templates/tests/test-connection.yaml"
)
for rel in "${required_files[@]}"; do
  [ -f "$CHART_DIR/$rel" ] || fail "missing charts/queryflux/$rel"
done

# Chart.yaml core fields.
grep -Eq '^apiVersion:[[:space:]]*v2[[:space:]]*$' "$CHART_DIR/Chart.yaml" || fail "Chart.yaml apiVersion must be v2"
grep -Eq '^name:[[:space:]]*queryflux[[:space:]]*$' "$CHART_DIR/Chart.yaml" || fail "Chart.yaml name must be queryflux"
grep -Eq '^type:[[:space:]]*application[[:space:]]*$' "$CHART_DIR/Chart.yaml" || fail "Chart.yaml type must be application"

# values.schema.json must be valid JSON (helm also enforces this on lint).
python3 -m json.tool "$CHART_DIR/values.schema.json" >/dev/null 2>&1 \
  || fail "values.schema.json is not valid JSON"

# Admin Secret must use configurable key names rather than hardcoded ones.
grep -q '{{ .Values.existingSecret.usernameKey }}' "$CHART_DIR/templates/secret.yaml" \
  || fail "templates/secret.yaml must use configurable admin Secret usernameKey"
grep -q '{{ .Values.existingSecret.passwordKey }}' "$CHART_DIR/templates/secret.yaml" \
  || fail "templates/secret.yaml must use configurable admin Secret passwordKey"

# Security defaults that the chart promises in its values.
grep -q 'runAsNonRoot: true' "$CHART_DIR/values.yaml" || fail "values.yaml must set runAsNonRoot: true"
grep -q 'readOnlyRootFilesystem: true' "$CHART_DIR/values.yaml" || fail "values.yaml must set readOnlyRootFilesystem: true"
grep -q -- '- ALL' "$CHART_DIR/values.yaml" || fail "values.yaml securityContext.capabilities.drop must include ALL"

# helm lint + template against default values and each example.
run_helm() {
  local label="$1"; shift
  local output
  if ! output="$("$@" 2>&1)"; then
    fail "$label failed:
$output"
  fi
}

run_helm "helm lint" helm lint "$CHART_DIR"
run_helm "helm template" helm template queryflux "$CHART_DIR"

for values_file in "$CHART_DIR"/examples/*.yaml; do
  run_helm "helm lint --values $values_file" helm lint "$CHART_DIR" --values "$values_file"
  run_helm "helm template --values $values_file" helm template queryflux "$CHART_DIR" --values "$values_file"
done

echo "helm chart check passed"
