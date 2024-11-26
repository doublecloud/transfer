{{- define "replication-statefulset" }}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ .Release.Name }}
  labels:
    app.kubernetes.io/name: {{ .Release.Name }}-replication
    {{- include "common.labels" . | nindent 4 }}
spec:
  serviceName: {{ .Release.Name }}
  replicas: {{ .Values.coordinator.job_count }}
  selector:
    matchLabels:
      app.kubernetes.io/name: {{ .Release.Name }}-replication
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {{ .Release.Name }}-replication
        {{- include "common.labels" . | nindent 8 }}
      {{- if .Values.podAnnotations }}
      annotations:
        {{- .Values.podAnnotations | toYaml | nindent 8 }}
      {{- end }}
    spec:
      {{- include "transfer.spec" (dict "commandType" "replicate" "Values" .Values "Release" .Release "Chart" .Chart) | nindent 6 }}
{{- end }}
