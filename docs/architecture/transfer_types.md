# Transfers

**Transfer** provides a convenient way to transfer data between DBMSes, object stores, and message brokers. Using {{ data-transfer-name }} reduces your migration period and minimizes downtime when switching to a new database. It can create a permanent replica of the database and automatically transfer the database schema from the source to the target.

Transfer creates a pipeline that connects [source](endpoints.md#source-endpoints) and [target](endpoints.md#target-endpoints) endpoints.

## Types

There are three types of transfers available at {{ DC }}:

* [Snapshot](#snapshot) moves a snapshot of the source to the target once.

* [Periodic snapshot](#periodic-snapshot) repeatedly moves a snapshot of the source to the target.

* [Replication](#replication) continuously receives changes from the source and applies them to the target while the initial data synchronization isn't performed.

* [Snapshot and replication](#snapshot-and-replication) transfers the current state of the source and then keeps it updated if changes occur.

## Transfer life cycles

### Snapshot

The **Snapshot** type transfers the state of the source database to the target on a single occasion. This transfer type doesn't constantly update the target database: changes that occur on the source after transfer is completed won't automatically copied to the target.

This transfer type is useful for the tasks with no writing load on the source, or when there is no need for constant target database updates.

When the transfer is ready, its status switches to `Snapshotting` throughout the data migration process from source to target. Upon completion, the transfer deactivates automatically, and acquires the `Done` status.

### Periodic snapshot

This type is the same as the `Snapshot` type but runs a transfer at the specified interval.

When the transfer is ready, its status switches to `Snapshotting` throughout the data migration process from source to target. Upon completion, the transfer deactivates automatically and acquires the `Done` status.

### Replication

The **Replication** type transfers changes from the source to the target without copying the complete dataset - only the data schema is transferred upon activation.

After the user activates the transfer, its status permanently changes to `Running`. All the changes that occur at the source are automatically transmitted to the target.

### Snapshot and replication

The **Snapshot and replication** type combines the **Snapshot** and **Replication** transfers: first, the service transfers all the source data to the target, and then it's automatically updated.

After activation, the status of the transfer changes to `Snapshotting`. This status will persist until all the data from the source is transferred to the target. Then the status will switch to `Running`. This means all the changes that occur on the source will be automatically transferred to the target.

## See also
