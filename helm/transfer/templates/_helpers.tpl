{{- define "data-upload-job.job" }}
{{- include "transfer/templates/_activate-job.yaml" . }}
{{- end }}

{{- define "data-upload-job.job-ondemand" }}
{{- include "transfer/templates/_activate-job-ondemand.yaml" . }}
{{- end }}

{{- define "data-upload-job.statefulset" }}
{{- include "transfer/templates/_replication-statefulset.yaml" . }}
{{- end }}

{{- define "transfer.spec" }}
{{- include "transfer/templates/_transfer_spec.yaml" . }}
{{- end}}
