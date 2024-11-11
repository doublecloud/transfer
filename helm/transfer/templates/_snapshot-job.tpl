{{- define "snapshot-job" }}
apiVersion: batch/v1
kind: Job
metadata:
  name: "{{ .Release.Name }}-snapshot"
  labels:
    app.kubernetes.io/name: {{ .Release.Name }}-snapshot
    {{- include "common.labels" . | nindent 4 }}
spec:
  completions: {{ .Values.coordinator.job_count }}
  parallelism: {{ .Values.coordinator.job_count }}
  completionMode: Indexed
  backoffLimit: 1
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {{ .Release.Name }}-snapshot
        {{- include "common.labels" . | nindent 8 }}
      {{- if .Values.podAnnotations }}
      annotations:
        {{- .Values.podAnnotations | toYaml | nindent 8 }}
      {{- end }}
    spec:
      restartPolicy: Never
      {{- include "transfer.spec" (dict "commandType" "activate" "Values" .Values "Release" .Release) | nindent 6 }}
{{- end }}
