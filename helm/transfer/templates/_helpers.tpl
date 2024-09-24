# templates/_helpers.tpl

{{- define "data-upload-job.job" }}
{{- include "transfer/templates/_activate-job.yaml" . }}
{{- end }}

{{- define "data-upload-job.statefulset" }}
{{- include "transfer/templates/_replication-statefulset.yaml" . }}
{{- end }}
