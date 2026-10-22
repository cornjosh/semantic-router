{{- define "adaptive.name" -}}{{ .Release.Name }}-router{{- end -}}
{{- define "adaptive.etcd" -}}{{ .Release.Name }}-etcd{{- end -}}
{{- define "adaptive.config" -}}
{{- $cfg := (.Values.serviceConfig | default (.Files.Get "files/service.yaml")) | fromYaml -}}
{{- if eq .Values.storage.backend "sqlite" -}}
  {{- if ne (int .Values.replicas) 1 -}}{{ fail "SQLite requires replicas=1; select etcd for cross-node HA" }}{{- end -}}
  {{- if .Values.etcd.enabled -}}{{ fail "etcd.enabled requires storage.backend=etcd" }}{{- end -}}
  {{- $_ := set $cfg "state" (dict "backend" "sqlite" "path" "/data/adaptive.db") -}}
{{- else if eq .Values.storage.backend "etcd" -}}
  {{- $endpoints := .Values.storage.endpoints -}}
  {{- if .Values.etcd.enabled -}}
    {{- $endpoints = list -}}
    {{- range $i := until 3 -}}
      {{- $endpoints = append $endpoints (printf "https://%s-%d.%s.%s.svc.cluster.local:2379" (include "adaptive.etcd" $) $i (include "adaptive.etcd" $) $.Release.Namespace) -}}
    {{- end -}}
  {{- end -}}
  {{- if not $endpoints -}}{{ fail "etcd requires storage.endpoints or etcd.enabled" }}{{- end -}}
  {{- $_ := required "etcd requires storage.clientTLSSecret" .Values.storage.clientTLSSecret -}}
  {{- $_ := set $cfg "state" (dict "backend" "etcd" "endpoints" $endpoints "prefix" .Values.storage.prefix "ca_file" "/etcd-tls/ca.crt" "cert_file" "/etcd-tls/tls.crt" "key_file" "/etcd-tls/tls.key") -}}
{{- else -}}{{ fail "storage.backend must be sqlite or etcd" }}{{- end -}}
{{- $cfg | toYaml -}}
{{- end -}}
