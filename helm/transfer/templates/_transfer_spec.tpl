{{- define "transfer.spec" -}}
serviceAccountName: {{ .Values.serviceAccount.externalName | default $.Release.Name }}
containers:
  - name: transfer
    image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default $.Chart.AppVersion }}"
    command:
      - "/usr/local/bin/trcli"
      - "{{ .commandType }}"
      - "--transfer"
      - "/volume-mounts/datatransfer/config/config.yaml"
      - "--coordinator"
      - "{{ .Values.coordinator.type }}"
      - "--coordinator-s3-bucket"
      - "{{ .Values.coordinator.bucket }}"
      - "--log-level"
      - "{{ .Values.log.level }}"
      - "--log-config"
      - "{{ .Values.log.config }}"
      - "--coordinator-job-count"
      - {{.Values.coordinator.job_count}}
      - "--coordinator-process-count"
      - {{.Values.coordinator.process_count}}
    {{- if .Values.env }}
    env:
      {{- range $name, $value := .Values.env }}
      - name: {{ $name }}
        value: {{ $value }}
      {{- end }}
    {{- end }}
    ports:
      - name: pprof
        protocol: TCP
        containerPort: 8080
      - name: health
        protocol: TCP
        containerPort: 3000
    {{- if .Values.podMonitor.enabled }}
      - name: prometheus
        containerPort: 9091
        protocol: TCP
    {{- end }}
    volumeMounts:
      - name: config
        mountPath: /volume-mounts/datatransfer/config
    {{- with .Values.resources }}
    resources:
    {{ toYaml . | nindent 4 }}
    {{- end }}
volumes:
  - name: config
    configMap:
      name: {{ $.Release.Name }}-config
      items:
        - key: config.yaml
          path: config.yaml
{{- if .Values.nodeSelector }}
nodeSelector:
  {{- toYaml .Values.nodeSelector | nindent 2 }}
{{- end }}
{{- if .Values.tolerations }}
tolerations:
  {{- toYaml .Values.tolerations | nindent 2 }}
{{- end }}
{{- end -}}
