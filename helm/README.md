# Transfer Helm Chart 

This Helm chart deploys a set of continuous replication service or one-time data upload job using Kubernetes. Chart is created for copying data to Clickhouse from many of different supported sources, but also can be used for any destination database. 

Each transfer could be configured separately as:

1. **INCREMENT_ONLY** or **SNAPSHOT_AND_INCREMENT**:  Provides continuous data replication. `StatefulSet` used because numeric pod indexing needed for coordinating different instances of trcli between pods. trcli make snapshot before replication if requested.
2. **SNAPSHOT_ONLY**: A one-time `Job` created for uploading a snapshot of data. Number of retries and parallel instances can be controlled. (not tested yet)
3. **SNAPSHOT_ONLY** with **cron_expression** defined: The same as above, but `CronJob` will be created for periodic snapshots. (not tested yet)

## Prerequisites

- Kubernetes 1.18+
- Helm 3.0+
- A Docker image with trcli, clickhouse-client and pg_dump  

## Chart Installation

### 1. Install the Helm chart

To install the chart, first run the following command:

```bash
helm pull oci://ghcr.io/doublecloud/transfer-helm/transfer
```
After editing the values.yaml file, you can install the chart:

```bash
helm install transfer ./transfer 
```

### 2. Uninstall the Helm chart

To uninstall the chart, run:

```bash
helm uninstall <release-name>
```

## Configuration

There are several configuration options available for the Helm chart. Several sets of Global variables, applied to all transfers, and transfer-specific variables are available.

### Global Parameters

```yaml
global:
  namespace: "altinity-cloud-managed-clickhouse"
  cluster: "prod"
  
image:    
  repository: ghcr.io/doublecloud/transfer
```

- cluster - is used to build the name of Kubernetes objects, like StatefulSet, Job, etc, and can be used inside ConfigMaps to build a hosts names and such. You can consider it as a namespace inside namespace. F.e.the same set of transfer jobs for stage and prod clusters can be deployed in the same namespace.
- namespace - is used to deploy the chart to a specific namespace.
- image - is the Docker image repo used for the transfer. The version is taken from the Chart.yaml file.

### Pod resource limits

You can adjust defaults the CPU and memory allocation for the pods using the `default_profile` section of the `values.yaml` file. Ensure you tune both `requests` and `limits` accordingly to expected event stream volume.

```
default_profile:
    limits:
      cpu: 2000m
      memory: 2048Mi
    requests:
      cpu: 100m
      memory: 256Mi
large_profile:
    limits:
      cpu: 4000m
      memory: 4096Mi
```      

Specific resource settings could be placed to named profiles (like `large_profile`) and can be referred in each transfer configuration by its name using the `resource_profile` field.

### Coordinator Configuration

The `coordinator_s3` section configures an external coordination service, that will be used for coordinating several Pods working on the same source and for storing the position of the replication process for sources that do not have it internally (like mongodb).

```yaml
coordinator_s3:
    bucket: some-bucket
    region: us-east-1
    serviceAccountName: clickhouse-backup
```
`serviceAccountName` needed for providing access to the bucket and should be created before deploying the Helm Chart.


### Transfer Configuration

```yaml
transfers:
# full set of parameters
  - name: test1
    type: INCREMENT_ONLY          # INCREMENT_ONLY (default), SNAPSHOT_ONLY, SNAPSHOT_AND_INCREMENT
    cron_expression: "0 1 * * *"  # for SNAPSHOT_ONLY. CronJob will be created instead of Job
    src:  mongodb                 # ref to db-hosts and Secrets
    dst:  clickhouse              # ref to db-hosts and Secrets
    job_count: 1                  # set more than one means work would be sharded, coordinator must be non memory
    process_count: 4              # how many threads will be run inside each job
    backoffLimit: 1
    resource_profile: large_profile # ref to profiles section
    mode: debug                   # enabled (default), disabled, debug
    log_level: info               # ("panic", "fatal", "error", "warning", "info", "debug")

# only necessary parameters
  - name: test2
    src: mongodb
    dst:  clickhouse

  - name: test3
    src: postgresdb
    dst:  clickhouse
```

The minimal set of parameters for each transfer is `name`, `src`, and `dst`. The `name` is used to build the name of the Kubernetes objects, like ConfigMap, StatefulSet, Job, etc. Also the `name` is used to read the transfer configuration from the `configs` directory.

The `src` and `dst` are references to the database hosts configs in `db-hosts` directory.  Those configs would be defaults for building src/dst config section of produced transfer.yaml (specific keys from config directory will overwrite them). Also  src/dst names is used for attaching Kubernetes `Secrets` thought environmental values (see below).

Other parameters are optional and can be used to tune the particular transfer behavior.


### Secrets

Secrets are used to hide database passwords and certificates and not managed ny this Helm Chart. Tuning Secret security (like using wallets, etc) is out of scope of this Helm Chart.

Secret will be used to get environment variables while creating the Pod.

Typical Secret file looks like:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: transfer-mongodb
  namespace: altinity-cloud-managed-clickhouse
type: Opaque
stringData:
  mongodb_PASSWORD: "XX"
  mongodb_TLSFILE: |
      -----BEGIN CERTIFICATE-----
      
      -----END CERTIFICATE-----
```

The name of the Secret should be the same as the `src` or `dst` name in the transfers configuration section. The prefix should be aligned with Chart name (in Chart.yaml) and the namespace should be the same as the namespace in the global section.

Please create and apply Secrets before deploying the Helm Chart.

### Advanced Pod parameters

If provided, those sections are included into StatefulSet and Job manifests as a whole to suitable places.

```yaml
annotations:
  prometheus.io/path: /metrics
  prometheus.io/port: '9091'
  prometheus.io/scrape: 'true'

ports:
  - name: prometheus
    protocol: TCP
    containerPort: 9091
  - name: pprof
    protocol: TCP
    containerPort: 8080
  - name: health
    protocol: TCP
    containerPort: 3000

affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: node.altinity.cloud/role.clickhouse
              operator: Exists
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
            - key: node.altinity.cloud/role.zookeeper
              operator: Exists
        topologyKey: kubernetes.io/hostname
```

## Debugging and Monitoring

### Debug mode
If you set `mode: debug` for particular transfer, all Kubernetes objects will be created, but instead of trcli, sh will be used as the entrypoint. So you can exec into Pod, manually run trcli to see what is going on, tune settings/config/etc. 

```
cd config
trcli replicate
trcli replicate --coordinator s3 --coordinator-s3-bucket bucket_name --log-level debug --log-config console --coordinator-job-count 1 --coordinator-process-count 4
```

You can override any configuration option in the `values.yaml` file by using the `--set` flag when installing the Helm chart. For example:

```bash
helm upgrade transfer ./ --set transfers.name.mode=debug
```

### Monitoring

The transfer service exposes Prometheus metrics on port 9091. You can access the metrics by port-forwarding the service to your local machine:


## Contributing

Feel free to open issues and submit PRs to improve this chart!




 
