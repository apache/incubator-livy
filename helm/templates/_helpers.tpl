{{/*
Expand the name of the chart.
*/}}
{{- define "livy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "livy.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}



{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "livy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "livy.labels" -}}
helm.sh/chart: {{ include "livy.chart" . }}
{{ include "livy.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "livy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "livy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "livy.serviceAccountName" -}}
{{- if .Values.serviceAccounts.livy.create }}
{{- default (include "livy.fullname" .) .Values.serviceAccounts.livy.name }}
{{- else }}
{{- default "default" .Values.serviceAccounts.livy.name }}
{{- end }}
{{- end }}

{{- define "livy.sparkServiceAccountName" -}}
{{- if .Values.serviceAccounts.spark.create }}
{{- default (printf "%s-spark" (include "livy.fullname" .)) .Values.serviceAccounts.spark.name }}
{{- else }}
{{- default "default" .Values.serviceAccounts.spark.name }}
{{- end }}
{{- end }}
