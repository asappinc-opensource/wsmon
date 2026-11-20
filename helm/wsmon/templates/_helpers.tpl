{{/* ======================================================================
Names and labels
====================================================================== */}}

{{- define "wsmon.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Release-wide name. Truncated to 63 chars (DNS label). If the release name
already contains the chart name it is used as is.
*/}}
{{- define "wsmon.fullname" -}}
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

{{/* Per-role resource names: <fullname>-server / <fullname>-client, or the role's fullnameOverride. */}}
{{- define "wsmon.server.fullname" -}}
{{- if .Values.server.fullnameOverride }}
{{- .Values.server.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-server" (include "wsmon.fullname" . | trunc 56 | trimSuffix "-") }}
{{- end }}
{{- end }}

{{- define "wsmon.client.fullname" -}}
{{- if .Values.client.fullnameOverride }}
{{- .Values.client.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-client" (include "wsmon.fullname" . | trunc 56 | trimSuffix "-") }}
{{- end }}
{{- end }}

{{- define "wsmon.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels of one role. The component label keeps the server Service
from selecting probe Pods and the monitors from cross-scraping.
Call with (dict "root" $ "component" "server").
*/}}
{{- define "wsmon.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wsmon.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/* Common labels of one role. Call with (dict "root" $ "component" "server"). */}}
{{- define "wsmon.labels" -}}
helm.sh/chart: {{ include "wsmon.chart" .root }}
{{ include "wsmon.selectorLabels" . }}
{{- if .root.Chart.AppVersion }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end }}

{{/* Labels of release-wide objects (the shared auth Secret). */}}
{{- define "wsmon.sharedLabels" -}}
helm.sh/chart: {{ include "wsmon.chart" . }}
app.kubernetes.io/name: {{ include "wsmon.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* ServiceAccount name of one role. Call with (dict "root" $ "component" "server"). */}}
{{- define "wsmon.serviceAccountName" -}}
{{- $role := index .root.Values .component }}
{{- if $role.serviceAccount.create }}
{{- default (include (printf "wsmon.%s.fullname" .component) .root) $role.serviceAccount.name }}
{{- else }}
{{- default "default" $role.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "wsmon.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion | toString) }}
{{- end }}

{{/* The auth Secret both roles read: an existing one, or the chart's own. */}}
{{- define "wsmon.authSecretName" -}}
{{- .Values.auth.secret.name | default (printf "%s-auth" (include "wsmon.fullname" .)) }}
{{- end }}

{{/* True ("true") when the chart creates the auth Secret itself (a role is enabled and the token is inline). */}}
{{- define "wsmon.authInline" -}}
{{- if and (or .Values.server.enabled .Values.client.enabled) .Values.auth.enabled (not .Values.auth.secret.name) }}true{{ end }}
{{- end }}

{{/* ======================================================================
Scalars and maps
====================================================================== */}}

{{/*
Render a YAML scalar as an env string. YAML numbers arrive as float64, which
would otherwise print as "1e+06"; whole numbers are printed as integers.
*/}}
{{- define "wsmon.scalar" -}}
{{- if kindIs "invalid" . }}
{{- else if kindIs "float64" . }}
{{- if eq . (floor .) }}{{ int64 . }}{{ else }}{{ . }}{{ end }}
{{- else }}
{{- . }}
{{- end }}
{{- end }}

{{/*
Shallow overlay of a list of maps: later maps win per key, including keys
whose value is false, 0 or "" (unlike sprig merge). Rendered as JSON so
callers can fromJson it. Call with (list $base $override ...).
*/}}
{{- define "wsmon.overlay" -}}
{{- $out := dict }}
{{- range $m := . }}
{{- range $k, $v := ($m | default dict) }}
{{- $_ := set $out $k $v }}
{{- end }}
{{- end }}
{{- toJson $out }}
{{- end }}

{{/*
A map rendered as WSMON_LABELS / WSMON_HEADERS / WSMON_URL_PARAMS: sorted by
name, "a=1,b=2". Empty when the map is empty.
*/}}
{{- define "wsmon.pairs" -}}
{{- $m := . | default dict }}
{{- $parts := list }}
{{- range $k := keys $m | sortAlpha }}
{{- $parts = append $parts (printf "%s=%s" $k (include "wsmon.scalar" (index $m $k))) }}
{{- end }}
{{- join "," $parts }}
{{- end }}

{{/*
WSMON_* passthrough env entries for a merged config map. Null and empty
values are skipped: the binary default applies. Call with the map.
*/}}
{{- define "wsmon.configEnv" -}}
{{- range $k, $v := . }}
{{- $s := include "wsmon.scalar" $v }}
{{- if ne $s "" }}
- name: {{ $k }}
  value: {{ $s | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Effective monitoring settings of one role: the top-level `monitoring`
overlaid key by key by <role>.monitoring. Rendered as JSON.
Call with (dict "root" $ "component" "server").
*/}}
{{- define "wsmon.monitoring" -}}
{{- $role := index .root.Values .component }}
{{- include "wsmon.overlay" (list .root.Values.monitoring ($role.monitoring | default dict)) }}
{{- end }}

{{/*
One ServiceMonitor endpoint / PodMonitor podMetricsEndpoint (a list item).
Call with (dict "mon" $mon "port" "metrics" "path" "/metrics").
*/}}
{{- define "wsmon.monitorEndpoint" -}}
- port: {{ .port }}
  path: {{ .path }}
  {{- with .mon.interval }}
  interval: {{ . }}
  {{- end }}
  {{- with .mon.scrapeTimeout }}
  scrapeTimeout: {{ . }}
  {{- end }}
  {{- with .mon.honorLabels }}
  honorLabels: {{ . }}
  {{- end }}
  {{- with .mon.metricRelabelings }}
  metricRelabelings:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .mon.relabelings }}
  relabelings:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}

{{/* ======================================================================
Server
====================================================================== */}}

{{/* Server config: top-level `config` overlaid by server.config. JSON. */}}
{{- define "wsmon.server.config" -}}
{{- include "wsmon.overlay" (list .Values.config .Values.server.config) }}
{{- end }}

{{/* The WebSocket path: WSMON_WS_PATH from the server config, else the binary default. */}}
{{- define "wsmon.server.wsPath" -}}
{{- $c := include "wsmon.server.config" . | fromJson }}
{{- (include "wsmon.scalar" (get $c "WSMON_WS_PATH")) | default "/__synthetic/ws" }}
{{- end }}

{{/* The metrics path: WSMON_METRICS_PATH from the server config, else the binary default. */}}
{{- define "wsmon.server.metricsPath" -}}
{{- $c := include "wsmon.server.config" . | fromJson }}
{{- (include "wsmon.scalar" (get $c "WSMON_METRICS_PATH")) | default "/metrics" }}
{{- end }}

{{/*
In-cluster URL of this release's server, for client probe urls:
  url: '{{ include "wsmon.server.wsURL" . }}'
*/}}
{{- define "wsmon.server.wsURL" -}}
{{- printf "ws://%s.%s.svc.cluster.local:%d%s" (include "wsmon.server.fullname" .) .Release.Namespace (int .Values.server.port) (include "wsmon.server.wsPath" .) }}
{{- end }}

{{/* Env var holding one labelsFromFields entry: POD_FIELD_<LABEL>. */}}
{{- define "wsmon.server.fieldEnv" -}}
{{- printf "POD_FIELD_%s" (upper .) }}
{{- end }}

{{/*
Server WSMON_LABELS: server.labels plus labelsFromFields as $(POD_FIELD_*)
references, sorted, "a=1,b=2".
*/}}
{{- define "wsmon.server.labels" -}}
{{- $m := deepCopy (.Values.server.labels | default dict) }}
{{- range $k, $_ := .Values.server.labelsFromFields | default dict }}
{{- $_ := set $m $k (printf "$(%s)" (include "wsmon.server.fieldEnv" $k)) }}
{{- end }}
{{- include "wsmon.pairs" $m }}
{{- end }}

{{/* ======================================================================
Client
====================================================================== */}}

{{/*
A merged map for one probe: client.<key> overlaid by the probe's <key>. Used
for labels, headers and urlParams. JSON.
Call with (dict "root" $ "probe" $probe "key" "labels").
*/}}
{{- define "wsmon.client.mergedMap" -}}
{{- include "wsmon.overlay" (list (index .root.Values.client .key) (index .probe .key)) }}
{{- end }}

{{/* Pairs string of a merged probe map. Same call as wsmon.client.mergedMap. */}}
{{- define "wsmon.client.pairs" -}}
{{- include "wsmon.pairs" (include "wsmon.client.mergedMap" . | fromJson) }}
{{- end }}

{{/* Probe config: top-level config, then client.config, then the probe's. JSON. Call with (dict "root" $ "probe" $probe). */}}
{{- define "wsmon.client.config" -}}
{{- include "wsmon.overlay" (list .root.Values.config .root.Values.client.config .probe.config) }}
{{- end }}

{{/* ======================================================================
Allow-lists, copied from internal/config. Keep them in sync with the Go
source; one line each, space-separated.
====================================================================== */}}

{{/*
Variables accepted in the top-level `config`: present in both ServerVars and
ClientVars with the same meaning (shared.go), minus the ones the chart sets
itself (WSMON_AUTH_TOKEN, WSMON_METRICS_ADDR, WSMON_LABELS) and WSMON_NAME
(one process's identity; a release-wide value would give every process the
same name).
*/}}
{{- define "wsmon.sharedEnv" -}}
WSMON_LOG_LEVEL WSMON_OTLP_ENDPOINT WSMON_TRACE_SAMPLE_RATIO WSMON_ENABLE_PERMESSAGE_DEFLATE WSMON_SHUTDOWN_TIMEOUT
{{- end }}

{{/* Server variables the chart sets itself; rejected in config. */}}
{{- define "wsmon.server.ownedEnv" -}}
WSMON_LISTEN_ADDR WSMON_METRICS_ADDR WSMON_AUTH_TOKEN WSMON_LABELS
{{- end }}

{{/* Client variables the chart sets itself; rejected in config. */}}
{{- define "wsmon.client.ownedEnv" -}}
WSMON_URL WSMON_MODE WSMON_LABELS WSMON_HEADERS WSMON_URL_PARAMS WSMON_METRICS_ADDR WSMON_RESOLVE_OVERRIDE WSMON_SNI_HOST WSMON_HOST_HEADER WSMON_AUTH_TOKEN WSMON_REMOTE_WRITE_URL WSMON_REMOTE_WRITE_BEARER_TOKEN WSMON_REMOTE_WRITE_USERNAME WSMON_REMOTE_WRITE_PASSWORD
{{- end }}

{{/* serverReservedLabels (server.go). */}}
{{- define "wsmon.server.reservedLabels" -}}
name version type code close_code
{{- end }}

{{/* clientReservedLabels (client.go). */}}
{{- define "wsmon.client.reservedLabels" -}}
mode name version go_version hash
{{- end }}

{{/* ======================================================================
Render-time validation. Only what the chart needs in order to render; value
validation is the binary's job. Rendered once, from
templates/validate.yaml.
====================================================================== */}}

{{/*
Checks a label map. Call with (dict "where" "server" "labels" $m "reserved" $list).
*/}}
{{- define "wsmon.validateLabels" -}}
{{- $where := .where }}
{{- $reserved := .reserved }}
{{- range $k, $v := .labels }}
{{- if or (not (regexMatch "^[a-zA-Z_][a-zA-Z0-9_]*$" $k)) (hasPrefix "__" $k) }}
{{- fail (printf "%s: invalid label name %q" $where $k) }}
{{- end }}
{{- if has $k $reserved }}
{{- fail (printf "%s: label name %q is reserved; the binary sets it itself" $where $k) }}
{{- end }}
{{- $s := include "wsmon.scalar" $v }}
{{- if or (eq $s "") (contains "," $s) }}
{{- fail (printf "%s: label %q must have a non-empty value without ','" $where $k) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Checks the keys of a config map. Call with
(dict "where" "server.config" "config" $m "owned" $list).
*/}}
{{- define "wsmon.validateConfigKeys" -}}
{{- $where := .where }}
{{- $owned := .owned }}
{{- range $k, $_ := .config | default dict }}
{{- if not (regexMatch "^WSMON_[A-Z0-9_]+$" $k) }}
{{- fail (printf "%s.%s: keys must be WSMON_* env names; put other variables in env" $where $k) }}
{{- end }}
{{- if has $k $owned }}
{{- fail (printf "%s.%s is set by the chart; use the dedicated value instead" $where $k) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "wsmon.validate" -}}
{{- $v := .Values }}
{{- /* ---- global (only reached when a role is enabled, see validate.yaml) ---- */}}
{{- if not $v.image.repository }}
{{- fail "image.repository is required (there is no registry default), e.g. <registry>/wsmon" }}
{{- end }}
{{- if and $v.auth.enabled (not $v.auth.secret.name) (not $v.auth.token) }}
{{- fail "auth.enabled requires auth.secret.name or auth.token" }}
{{- end }}
{{- if and $v.auth.enabled (not $v.auth.secret.key) }}
{{- fail "auth.enabled requires auth.secret.key" }}
{{- end }}
{{- $shared := include "wsmon.sharedEnv" . | splitList " " }}
{{- range $k, $_ := $v.config | default dict }}
{{- if not (regexMatch "^WSMON_[A-Z0-9_]+$" $k) }}
{{- fail (printf "config.%s: keys must be WSMON_* env names; put other variables in a role's env" $k) }}
{{- end }}
{{- if not (has $k $shared) }}
{{- fail (printf "config.%s is not a shared variable; top-level config accepts only %s. Put role-only keys in server.config or client.config" $k (join ", " $shared)) }}
{{- end }}
{{- end }}
{{- range $role := list "server" "client" }}
{{- if (index $v $role).enabled }}
{{- $mon := include "wsmon.monitoring" (dict "root" $ "component" $role) | fromJson }}
{{- if not (has $mon.type (list "serviceMonitor" "podMonitor" "none")) }}
{{- fail (printf "%s: monitoring.type %q must be serviceMonitor, podMonitor or none" $role $mon.type) }}
{{- end }}
{{- end }}
{{- end }}

{{- /* ---- server ---- */}}
{{- if $v.server.enabled }}
{{- include "wsmon.validateConfigKeys" (dict "where" "server.config" "config" $v.server.config "owned" (include "wsmon.server.ownedEnv" . | splitList " ")) }}
{{- $reserved := include "wsmon.server.reservedLabels" . | splitList " " }}
{{- include "wsmon.validateLabels" (dict "where" "server.labels" "labels" ($v.server.labels | default dict) "reserved" $reserved) }}
{{- range $k, $path := $v.server.labelsFromFields | default dict }}
{{- if or (not (regexMatch "^[a-zA-Z_][a-zA-Z0-9_]*$" $k)) (hasPrefix "__" $k) }}
{{- fail (printf "server.labelsFromFields: invalid label name %q" $k) }}
{{- end }}
{{- if has $k $reserved }}
{{- fail (printf "server.labelsFromFields: label name %q is reserved; the binary sets it itself" $k) }}
{{- end }}
{{- if not $path }}
{{- fail (printf "server.labelsFromFields.%s: fieldPath is required" $k) }}
{{- end }}
{{- if hasKey ($v.server.labels | default dict) $k }}
{{- fail (printf "server.labelsFromFields.%s: also set in server.labels; set it in only one" $k) }}
{{- end }}
{{- end }}
{{- if eq (int $v.server.port) (int $v.server.metricsPort) }}
{{- fail "server: port and metricsPort must differ" }}
{{- end }}
{{- end }}

{{- /* ---- client ---- */}}
{{- if $v.client.enabled }}
{{- $c := $v.client }}
{{- if not $c.probes }}
{{- fail "client.probes is empty; a client vantage needs at least one probe" }}
{{- end }}
{{- $owned := include "wsmon.client.ownedEnv" . | splitList " " }}
{{- $reserved := include "wsmon.client.reservedLabels" . | splitList " " }}
{{- include "wsmon.validateConfigKeys" (dict "where" "client.config" "config" $c.config "owned" $owned) }}
{{- $mon := include "wsmon.monitoring" (dict "root" $ "component" "client") | fromJson }}
{{- if and $c.remoteWrite.url (ne $mon.type "none") }}
{{- fail (printf "client.remoteWrite.url requires monitoring.type none for the client (effective type is %q); scrape or push, never both" $mon.type) }}
{{- end }}
{{- $names := dict }}
{{- $series := dict }}
{{- range $i, $p := $c.probes }}
{{- $where := printf "client.probes[%d]" $i }}
{{- if not $p.name }}
{{- fail (printf "%s: name is required" $where) }}
{{- end }}
{{- $where = printf "client.probes[%d] (%s)" $i $p.name }}
{{- if or (gt (len $p.name) 63) (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $p.name)) }}
{{- fail (printf "%s: name must be a DNS label (lowercase alphanumerics and '-', <= 63 chars)" $where) }}
{{- end }}
{{- if hasKey $names $p.name }}
{{- fail (printf "%s: duplicate probe name" $where) }}
{{- end }}
{{- $_ := set $names $p.name true }}
{{- if not $p.url }}
{{- fail (printf "%s: url is required" $where) }}
{{- end }}
{{- $mode := $p.mode | default "pingpong" }}
{{- if not (has $mode (list "pingpong" "burst" "idle")) }}
{{- fail (printf "%s: mode %q must be pingpong, burst or idle" $where $mode) }}
{{- end }}
{{- $labels := include "wsmon.client.mergedMap" (dict "root" $ "probe" $p "key" "labels") | fromJson }}
{{- include "wsmon.validateLabels" (dict "where" $where "labels" $labels "reserved" $reserved) }}
{{- range $field := list "headers" "urlParams" }}
{{- range $k, $val := include "wsmon.client.mergedMap" (dict "root" $ "probe" $p "key" $field) | fromJson }}
{{- if or (eq $k "") (contains "," $k) (contains "=" $k) }}
{{- fail (printf "%s: %s name %q must be non-empty without ',' or '='" $where $field $k) }}
{{- end }}
{{- if contains "," (include "wsmon.scalar" $val) }}
{{- fail (printf "%s: %s %q must have a value without ','" $where $field $k) }}
{{- end }}
{{- end }}
{{- end }}
{{- $key := printf "%s|%s" $mode (include "wsmon.pairs" $labels) }}
{{- if hasKey $series $key }}
{{- fail (printf "%s: same labels and mode as %s; their series would collide" $where (get $series $key)) }}
{{- end }}
{{- $_ := set $series $key $p.name }}
{{- include "wsmon.validateConfigKeys" (dict "where" (printf "%s: config" $where) "config" $p.config "owned" $owned) }}
{{- end }}
{{- end }}
{{- end }}
