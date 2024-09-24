Here is a `README.md` file for your Helm chart that explains the purpose, usage, configuration, and deployment steps for users:

```markdown
# Data Upload Helm Chart

This Helm chart deploys a one-time data upload job or continuous replication service using Kubernetes. It supports three deployment types:

1. **SNAPSHOT_ONLY**: A one-time `Job` for uploading a snapshot of data.
2. **INCREMENT_ONLY**: A continuous data replication `StatefulSet`.
3. **SNAPSHOT_AND_INCREMENT**: A `Job` for a one-time data snapshot upload followed by a continuous replication `StatefulSet` after the job completes.

## Prerequisites

- Kubernetes 1.18+
- Helm 3.0+
- A Docker image with the required commands:
  - `trctl activate --file /etc/config/config.yaml` for the snapshot job.
  - `trctl replication --file /etc/config/config.yaml` for continuous replication.

## Chart Installation

### 1. Install the Helm chart

To install the chart, run the following command:

```bash
helm install <release-name> ./transfer
```

For example:

```bash
helm install transfer ./transfer --set transferSpec.type=SNAPSHOT_ONLY
```

### 2. Uninstall the Helm chart

To uninstall the chart, run:

```bash
helm uninstall <release-name>
```

## Configuration

The chart is highly configurable. You can specify various parameters in the `values.yaml` file or through the `--set` option when running the `helm install` command.

### Parameters

| Parameter                   | Description                                                                      | Default                  |
|-----------------------------|----------------------------------------------------------------------------------|--------------------------|
| `transferSpec.id`           | Unique ID for the data transfer job.                                             | `dtttest`                |
| `transferSpec.type`         | Type of deployment: `SNAPSHOT_ONLY`, `INCREMENT_ONLY`, `SNAPSHOT_AND_INCREMENT`. | `SNAPSHOT_ONLY`          |
| `transferSpec.src.type`     | Source type (e.g., `pg`).                                                        | `pg`                     |
| `transferSpec.src.params`   | Source parameters.                                                               | `{}`                     |
| `transferSpec.dst.type`     | Destination type (e.g., `ch`).                                                   | `ch`                     |
| `transferSpec.dst.params`   | Destination parameters.                                                          | `{}`                     |
| `snapshot.worker_count`     | Number of parallel instances for the snapshot job.                               | `1`                      |
| `replication.worker_count`  | Number of replicas for the continuous replication `StatefulSet`.                 | `1`                      |
| `resources.requests.cpu`    | CPU resource requests for the pods.                                              | `100m`                   |
| `resources.requests.memory` | Memory resource requests for the pods.                                           | `128Mi`                  |
| `resources.limits.cpu`      | CPU resource limits for the pods.                                                | `500m`                   |
| `resources.limits.memory`   | Memory resource limits for the pods.                                             | `256Mi`                  |

### Example `values.yaml`

```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "256Mi"
    cpu: "500m"

transferSpec:
  id: dtttest
  type: SNAPSHOT_AND_INCREMENT  # Options: SNAPSHOT_ONLY, INCREMENT_ONLY, SNAPSHOT_AND_INCREMENT
  src:
    type: pg
    params:
      {
        "host": "postgres-source-host",
        "port": "5432",
        "username": "user",
        "password": "pass",
        "database": "db_name"
      }
  dst:
    type: ch
    params:
      {
        "host": "clickhouse-dest-host",
        "port": "9000",
        "username": "user",
        "password": "pass",
        "database": "db_name"
      }

# snapshot specific configurations
snapshot:
    worker_count: 4  # Number of parallel job instances

# replication specific configurations
replication:
    worker_count: 1  # Number of replicas
```

### Transfer Spec Configuration

The `transferSpec` section configures the data source (`src`) and destination (`dst`). These fields accept JSON-formatted strings in the `params` section for both source and destination configurations. The `type` defines the kind of source/destination system (e.g., PostgreSQL, ClickHouse).

- **Source Example**:

```yaml
src:
  type: pg  # Postgres source type
  params:
    {
      "host": "postgres-source-host",
      "port": "5432",
      "username": "user",
      "password": "pass",
      "database": "db_name"
    }
```

- **Destination Example**:

```yaml
dst:
  type: ch  # Clickhouse destination type
  params:
    {
      "host": "clickhouse-dest-host",
      "port": "9000",
      "username": "user",
      "password": "pass",
      "database": "db_name"
    }
```

## Deployment Types

### 1. **SNAPSHOT_ONLY**

This mode deploys a one-time `Job` to upload a data snapshot. You can control the number of parallel job instances by setting `job.instanceCount`.

To deploy:

```bash
helm install transfer ./transfer --set transferSpec.type=SNAPSHOT_ONLY --set job.instanceCount=3
```

### 2. **INCREMENT_ONLY**

This mode deploys a `StatefulSet` for continuous data replication. You can control the number of replicas with `statefulSet.replicaCount`.

To deploy:

```bash
helm install transfer ./transfer --set transferSpec.type=INCREMENT_ONLY --set statefulSet.replicaCount=2
```

### 3. **SNAPSHOT_AND_INCREMENT**

This mode first runs a `Job` to take a data snapshot, followed by a `StatefulSet` for continuous replication. The `StatefulSet` will only start after the `Job` completes successfully.

To deploy:

```bash
helm install transfer ./transfer --set transferSpec.type=SNAPSHOT_AND_INCREMENT --set job.instanceCount=2 --set statefulSet.replicaCount=2
```

## Resource Allocation

You can adjust the CPU and memory allocation for the pods using the `resources` section of the `values.yaml` file. Ensure you specify both `requests` and `limits` for better resource management.

### Example:

```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "256Mi"
    cpu: "500m"
```

## Advanced Deployment Options

### Setting Custom Values

You can override any configuration option in the `values.yaml` file by using the `--set` flag when installing the Helm chart. For example:

```bash
helm install transfer ./transfer --set transferSpec.id=my-custom-id --set transferSpec.type=INCREMENT_ONLY
```

### Monitoring and Logs

To view logs of a running `Job` or `StatefulSet` pod, you can use `kubectl`:

```bash
kubectl logs <pod-name>
```

For example, to get logs from the `Job`:

```bash
kubectl get pods -l job-name=transfer
kubectl logs <pod-name>
```

For the `StatefulSet`:

```bash
kubectl get pods -l statefulset.kubernetes.io/pod-name=data-upload-statefulset
kubectl logs <pod-name>
```

## Contributing

Feel free to open issues and submit PRs to improve this chart!

