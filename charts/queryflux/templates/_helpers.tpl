{{/*
Expand the name of the chart.
*/}}
{{- define "queryflux.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "queryflux.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "queryflux.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "queryflux.labels" -}}
helm.sh/chart: {{ include "queryflux.chart" . }}
{{ include "queryflux.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "queryflux.selectorLabels" -}}
app.kubernetes.io/name: {{ include "queryflux.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service account name.
*/}}
{{- define "queryflux.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "queryflux.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
ConfigMap name.
*/}}
{{- define "queryflux.configMapName" -}}
{{- default (printf "%s-config" (include "queryflux.fullname" .)) .Values.config.existingConfigMap -}}
{{- end -}}

{{/*
Admin Secret name.
*/}}
{{- define "queryflux.secretName" -}}
{{- default (printf "%s-admin" (include "queryflux.fullname" .)) .Values.existingSecret.name -}}
{{- end -}}
