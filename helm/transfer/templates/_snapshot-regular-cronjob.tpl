{{- define "snapshot-regular-cronjob" }}
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ .Release.Name }}-snapshot-regular
  labels:
    app.kubernetes.io/name: {{ .Release.Name }}-snapshot-regular
    {{- include "common.labels" . | nindent 4 }}
spec:
  suspend: {{ not .Values.transferSpec.regular_snapshot.enabled }}
  schedule: {{ .Values.transferSpec.regular_snapshot.cron_expression }}
  jobTemplate:
    metadata:
      labels:
        app.kubernetes.io/name: {{ .Release.Name }}-snapshot-regular
        {{- include "common.labels" . | nindent 8 }}
    spec:
      completions: {{ .Values.coordinator.job_count }}
      parallelism: {{ .Values.coordinator.job_count }}
      completionMode: Indexed
      backoffLimit: 6
      template:
        metadata:
          labels:
            app.kubernetes.io/name: {{ .Release.Name }}-snapshot-regular
            {{- include "common.labels" . | nindent 12 }}
          {{- if .Values.podAnnotations }}
          annotations:
            {{- .Values.podAnnotations | toYaml | nindent 12 }}
          {{- end }}
        spec:
          restartPolicy: Never
          {{- include "transfer.spec" (dict "commandType" "activate" "Values" .Values "Release" .Release) | nindent 10 }}
{{- end }}
