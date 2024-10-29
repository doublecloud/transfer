# Transfer Manager Operator

The **Transfer Manager Operator** is designed to manage and orchestrate data transfers in Kubernetes using custom resources. It automates the creation of Kubernetes resources such as ConfigMaps, CronJobs, and StatefulSets based on user-defined transfer types like snapshots and replications.

## Description

The operator is built using [Kubebuilder](https://book.kubebuilder.io/introduction.html) and is responsible for managing data transfers between different storage solutions. It defines a `Transfer` Custom Resource Definition (CRD) that allows users to configure the source and destination of the transfer, specify the type of transfer, and manage the lifecycle of the transfer resources.

The operator currently supports:
- **Snapshot Transfers**: A one-time data transfer, include regular transfers with increment
- **Replication Transfers**: Continuous replication between a source and a destination.
- **Combined Snapshot and Replication**: A hybrid approach that first transfers a snapshot, then sets up replication for ongoing updates.

## Getting Started

### Prerequisites

- **Go** version v1.22.0+
- **Docker** version 17.03+.
- **Kubectl** version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### Installation & Deployment

Follow the steps below to deploy the operator to your Kubernetes cluster.

#### Build and Push Your Image

First, build and push the Docker image that will be used to deploy the operator:

```sh
make docker-build docker-push IMG=<some-registry>/transfer-manager-operator:tag
```

> **NOTE:** Ensure that you have access to the specified container registry and the necessary permissions to push images.

#### Install the CRDs

Next, install the Custom Resource Definitions (CRDs) that define the `Transfer` resource:

```sh
make install
```

#### Deploy the Operator

Deploy the operator using the image built in the previous step:

```sh
make deploy IMG=<some-registry>/transfer-manager-operator:tag
```

If you encounter any RBAC-related issues, ensure that you have appropriate permissions (e.g., cluster-admin privileges).

### Creating Transfer Instances

To create instances of your `Transfer` resources, apply the sample manifests provided:

```sh
kubectl apply -k config/samples/
```

The samples include different types of transfers like **snapshot** and **replication**. Ensure that the sample values align with your testing needs.

#### Example Custom Resource (CR)

Here’s an example of how you can define a `Transfer` custom resource for a snapshot transfer:

```yaml
apiVersion: transfer.a.yandex-team.ru/v1
kind: Transfer
metadata:
  name: my-snapshot-transfer
spec:
  type: snapshot
  src:
    type: s3
    params: '{"bucket":"source-bucket"}'
  dst:
    type: s3
    params: '{"bucket":"destination-bucket"}'
```

For replication:

```yaml
apiVersion: transfer.a.yandex-team.ru/v1
kind: Transfer
metadata:
  name: my-replication-transfer
spec:
  type: replication
  src:
    type: s3
    params: '{"bucket":"source-bucket"}'
  dst:
    type: s3
    params: '{"bucket":"destination-bucket"}'
```

### Uninstalling the Operator

To remove the operator and its associated resources from your cluster, follow these steps:

#### Delete Transfer Instances

First, remove all `Transfer` instances from the cluster:

```sh
kubectl delete -k config/samples/
```

#### Uninstall CRDs

Uninstall the CRDs by running:

```sh
make uninstall
```

#### Undeploy the Operator

Finally, undeploy the operator from your Kubernetes cluster:

```sh
make undeploy
```

## Project Structure and Components

The `Transfer`-CRD in this project manages the lifecycle of the `Transfer` resource. Depending on the `spec.type` in the custom resource, the controller will create one or more Kubernetes resources:

- **ConfigMap**: Stores configuration data for the transfer.
- **CronJob**: Used for snapshot transfers, where a one-time data transfer is scheduled.
- **StatefulSet**: Used for replication transfers, where continuous data replication occurs between a source and destination.

### Features and Implementation Details

1. **Snapshot Transfers**: When `spec.type` is set to `snapshot`, a CronJob is created that handles the transfer of a snapshot from the source to the destination.
2. **Replication Transfers**: When `spec.type` is set to `replication`, the operator creates a StatefulSet to continuously sync data from the source to the destination.
3. **Hybrid Transfers**: For `snapshot_and_replication`, the operator first performs a snapshot and then configures replication after the snapshot is complete.

The operator monitors the transfer process and updates the status of the `Transfer` resource accordingly. You can check the status of your transfer by running:

```sh
kubectl get transfer <transfer-name> -o yaml
```

## Contributing

We welcome contributions from the community! Here’s how you can contribute to this project:

1. **Fork the repository** and clone it locally.
2. **Create a branch** for your feature or bugfix:
   ```sh
   git checkout -b feature/new-feature
   ```
3. **Implement your changes**, including tests.
4. **Run the tests** to ensure nothing is broken:
   ```sh
   make test
   ```
5. **Submit a pull request** with a detailed description of your changes.

Please make sure your code follows the project's style guidelines and is well-tested.

### Running the Operator Locally

If you want to test or develop the operator locally, you can run it directly on your machine using:

```sh
make run
```

This will run the operator outside of the Kubernetes cluster, useful for local development and debugging. Ensure you have the correct kubeconfig context set for your local cluster.

## License

This project is licensed under the terms of the Apache 2.0 License.
