{{- define "common.labels" -}}
app.kubernetes.io/component: {{ .Release.Name }}
{{- if .Values.commonLabels -}}
    {{- .Values.commonLabels | toYaml | nindent 0 }}
{{- end -}}
{{- end -}}
