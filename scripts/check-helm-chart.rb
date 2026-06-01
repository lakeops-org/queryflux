#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "open3"
require "yaml"

ROOT = File.expand_path("..", __dir__)
CHART_DIR = File.join(ROOT, "charts", "queryflux")

def fail_check(message)
  warn "helm chart check failed: #{message}"
  exit 1
end

def require_file(path)
  fail_check("missing #{path.sub("#{ROOT}/", "")}") unless File.file?(path)
end

def load_yaml(path)
  YAML.safe_load(File.read(path), aliases: true) || {}
rescue Psych::Exception => e
  fail_check("invalid YAML in #{path.sub("#{ROOT}/", "")}: #{e.message}")
end

def dig_value(hash, *keys)
  keys.reduce(hash) { |memo, key| memo.is_a?(Hash) ? memo[key] : nil }
end

required_files = [
  "Chart.yaml",
  "README.md",
  "examples/external-config-values.yaml",
  "examples/production-values.yaml",
  "values.yaml",
  "values.schema.json",
  "templates/_helpers.tpl",
  "templates/deployment.yaml",
  "templates/service.yaml",
  "templates/configmap.yaml",
  "templates/secret.yaml",
  "templates/serviceaccount.yaml",
  "templates/ingress.yaml",
  "templates/hpa.yaml",
  "templates/pdb.yaml",
  "templates/networkpolicy.yaml",
  "templates/servicemonitor.yaml",
  "templates/tests/test-connection.yaml"
]

required_files.each { |relative| require_file(File.join(CHART_DIR, relative)) }

chart = load_yaml(File.join(CHART_DIR, "Chart.yaml"))
fail_check("Chart.yaml apiVersion must be v2") unless chart["apiVersion"] == "v2"
fail_check("Chart.yaml name must be queryflux") unless chart["name"] == "queryflux"
fail_check("Chart.yaml type must be application") unless chart["type"] == "application"

values = load_yaml(File.join(CHART_DIR, "values.yaml"))
schema_path = File.join(CHART_DIR, "values.schema.json")
JSON.parse(File.read(schema_path))

Dir.glob(File.join(CHART_DIR, "examples", "*.yaml")).each { |path| load_yaml(path) }

expected_defaults = {
  %w[image repository] => "ghcr.io/lakeops-org/queryflux",
  %w[image pullPolicy] => "IfNotPresent",
  %w[service type] => "ClusterIP",
  %w[service ports trinoHttp port] => 8080,
  %w[service ports admin port] => 9000,
  %w[service ports studio port] => 3000,
  %w[config create] => true,
  %w[config mountPath] => "/etc/queryflux",
  %w[config fileName] => "config.yaml",
  %w[persistence type] => "inMemory",
  %w[podSecurityContext runAsNonRoot] => true,
  %w[securityContext allowPrivilegeEscalation] => false,
  %w[securityContext readOnlyRootFilesystem] => true,
  %w[networkPolicy enabled] => false,
  %w[serviceMonitor enabled] => false
}

expected_defaults.each do |path, expected|
  actual = dig_value(values, *path)
  fail_check("values.yaml #{path.join(".")} expected #{expected.inspect}, got #{actual.inspect}") unless actual == expected
end

drop_caps = dig_value(values, "securityContext", "capabilities", "drop")
fail_check("values.yaml securityContext.capabilities.drop must include ALL") unless Array(drop_caps).include?("ALL")

secret_template = File.read(File.join(CHART_DIR, "templates", "secret.yaml"))
unless secret_template.include?("{{ .Values.existingSecret.usernameKey }}") &&
       secret_template.include?("{{ .Values.existingSecret.passwordKey }}")
  fail_check("templates/secret.yaml must use configurable admin Secret key names")
end

required_value_keys = %w[
  adminCredentials
  affinity
  autoscaling
  env
  envFrom
  existingSecret
  extraContainers
  extraVolumeMounts
  extraVolumes
  fullnameOverride
  ingress
  lifecycle
  livenessProbe
  nodeSelector
  pdb
  podAnnotations
  podLabels
  readinessProbe
  resources
  tolerations
  topologySpreadConstraints
]
missing_keys = required_value_keys.reject { |key| values.key?(key) }
fail_check("values.yaml missing extension keys: #{missing_keys.join(", ")}") unless missing_keys.empty?

if system("command -v helm >/dev/null 2>&1")
  [["lint", CHART_DIR], ["template", "queryflux", CHART_DIR]].each do |args|
    output, status = Open3.capture2e("helm", *args)
    fail_check("helm #{args.join(" ")} failed:\n#{output}") unless status.success?
  end

  Dir.glob(File.join(CHART_DIR, "examples", "*.yaml")).sort.each do |values_file|
    [["lint", CHART_DIR, "--values", values_file],
     ["template", "queryflux", CHART_DIR, "--values", values_file]].each do |args|
      output, status = Open3.capture2e("helm", *args)
      fail_check("helm #{args.join(" ")} failed:\n#{output}") unless status.success?
    end
  end
else
  warn "helm not found; skipped helm lint/template"
end

puts "helm chart check passed"
