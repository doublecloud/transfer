# Transfer Manager

**a.k.a Data Transfer or simply Transfer**

**Transfer** provides a convenient way to transfer data between DBMSes, object stores, message brokers or anything that stores data.
Our ultimate mission is to help you move data from any source to any destination with fast, effective and easy-to-use tool.

Essentially we are building [no-code (or low-code)](https://en.wikipedia.org/wiki/No-code_development_platform) [EL(T)](https://airbyte.com/blog/elt-pipeline) service that can scale data pipelines from several megabytes of data to dozens of petabytes without hassle.

**Transfer** divided into 2 main parts:

1. Control Plane - this is our server and API that store user input and control pipelines
2. Data Plane - this is our main value, actual **connector**-s (or **provider**-s)

Control Plane essentially a [GRPC](https://grpc.io/)-api that store in [PostgreSQL](https://www.postgresql.org/) configuration of user transfers and manage their runs in configured environments.

# Control Plane Concepts

Control plane is [resource based api](https://cloud.google.com/apis/design/resources), we have 2 main resource:
1. [Endpoint](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/endpoint_service.proto?rev=r10460194#L45) - specific data storage configuration. Can be source or target. Contain user sensitive data such as passwords, essentially connection info + some settings.
2. [Transfer](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/transfer_service.proto?rev=r10470906#L26) - pair of source + target endpoint with extra settings such as [Runtime](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/transfer.proto?rev=r10470906#L134) and [Transformer](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/transfer.proto?rev=r10470906#L958) and some [more](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/transfer.proto?rev=r10470906#L35-39).

Each resource on mutate operation returns [Operation](https://a.yandex-team.ru/arcadia/cloud/bitbucket/private-api/yandex/cloud/priv/operation/operation.proto?rev=r9084579#L13) object that can do some work async.

For all installation we share same [Private proto API](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/?rev=r10624348).
We heavily rely on proto-codegen, therefore we have several [proto-plugins](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/ya.make?rev=r9786476#L78-81)
We are not committing proto-generated code.

We have separate layer of [Console API-s](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/console/?rev=r10624348) for UI in all installations.
We have developed specific [Form](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/console/form/readme.md?rev=r9681904#L1) protocol that allow us to customize use input appearance based on out proto-models.

### Worker

Among with API we deploy **scheduler** (or **worker**). It lives in the code [here](https://a.yandex-team.ru/arc_vcs/transfer_manager/go/pkg/worker/?rev=r8527864).
Worker main task is planning and monitoring user data planes. Data Plane divided into 2 main unit of works:
1. **Task** - finite piece of work that has start and end. All tasks declared [here](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/worker/tasks/task_visitor.go?rev=r10345670#L25)
2. **Replication** - infinite streaming job. The only way to stop replication - spawn a task that change [**status**](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/server/model_transfer_status.go?rev=r8674055#L8) of a **transfer**.
The key difference between a **task** and a **replication** is that it is finite (there is no need to restart it if something went wrong).
Task status is stored [here](https://a.yandex-team.ru/arc_vcs/transfer_manager/go/pkg/worker/pool.go?rev=r8525626#L38) in memory, and synced from database.
For each new tasks we call [this](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/task_scheduler.go?rev=r10078494#L234) method, which store new task for scheduling.
Once task grabbed by worker for scheduling we call task [Factory Func](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/task_factory.go?rev=r10125546#L29). This will create corresponding artifact in configured runtime (for example virtual machine).
Once task is completed or failed we call [Stop](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/abstract/runtime.go?rev=r10319644#L124) method which cleanup every artifact from runtime.

For replication workflow is similar. We look for [Running](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/worker/pool.go?rev=r10392729#L429) replications and call replication [Factory Func](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/replication_factory.go?rev=r9868991#L25).

For each runtime we have 4 structs:
1. Replication Control Plane ([k8s example](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/k8s/replication_k8s_cp.go?rev=r10149811)) - creating artifacts of runtime specific object (virtual machine, pod, etc...). Also responsible for restarting instances if something went wrong.
2. Replication Data Plane ([k8s example](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/k8s/replication_k8s_dp.go?rev=r10579013)) - code that would be run inside a data plane worker.
3. Task Control Plane ([k8s example](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/k8s/task_k8s_cp.go?rev=r10149811)) - creating artifacts of runtime specific object (virtual machine, pod, etc...).
4. Task Data Plane ([k8s example](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/runtime/k8s/task_k8s_dp.go?rev=r10345670#L34)) - code that would be run inside data plane task.

### High Level Architecture

![architecture](https://jing.yandex-team.ru/files/tserakhau/perfecto.drawio.svg)

Dependencies:

1. Console - our UI, faced to internet
2. [IAM](https://docs.yandex-team.ru/iam-cookbook/2.authentication_and_authorization/authz_concepts) - authorization system.
3. Monitoring - system that store monitoring (Cloud Monitoring or Prometheus)
4. Log Storage - system that store logs (YDB or Clickhouse)
5. KMS - something for encryption (Cloud KMS or AWS KMS)
6. Logs Queue - some message broker (YDS or Logbroker or Kafka)
7. Runtime API - something for running runtime aritfacts (AWS EC2 api, Cloud Instance Group API, YT or K8S cluster)

For our own infrastructure we utilize [dog-fooding](https://en.wikipedia.org/wiki/Eating_your_own_dog_food) concept. So our logs delivered to our log storage via **Transfer**.

### Installations

We currently maintain 4 installations:

1. Yandex Internal (or simply internal [prod](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/internal_prod.yaml?rev=r10454585) / [testing](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/internal_testing.yaml?rev=r10454585)). UI - [prod](https://yc.yandex-team.ru/) / [testing](https://yc-test.yandex-team.ru/)
2. Yandex Cloud Ru (or simply external) [prod](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/external_prod.yaml?rev=r10580701) / [pre-prod](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/external_preprod.yaml?rev=r10580701). UI - [prod](https://console.cloud.yandex.ru/) / [preprod](https://console-preprod.cloud.yandex.ru/)
3. Yandex Cloud Israel (or simply israel) [prod](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/israel.yaml?rev=r10580701). UI - [prod](https://console.il.nebius.com)
4. Double Cloud (or simply aws) [prod](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/aws_prod.yaml?rev=r10454585) / [pre-prod](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/config/controlplane/installations/aws_preprod.yaml?rev=r10454585). UI - [prod](http://app.double.cloud/data-transfer) / [pre-prod](https://app.yadc.io/data-transfer)


# Data Plane Concepts

Data Plane is a golang pluggable package that include into data-plane binary and register itself into it. Our data-plane plugins can be one of:

1. [Storage](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/abstract/storage.go?rev=r10238003#L385) - one-time data reader
2. [Sink](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/abstract/async_sink.go?rev=r9756859#L12) - data writer
3. [Source](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/abstract/source.go?rev=r9904810#L47) - streaming data reader
4. [Provider](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/base/transfer.go?rev=r10323244#L171) - one-time data reader but made a bit different
5. [Target](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/base/transfer.go?rev=r10323244#L166) - data writer but made a bit different

Data pipeline composes with two [Endpoint](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/server/endpoint_params.go?rev=0dc174c31b#L45)-s: [Source](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/server/endpoint_params.go?rev=0dc174c31b#L44) and [Destination](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/server/endpoint_params.go?rev=0dc174c31b#L49).
Each Data pipeline essentially link between **Source** {`Storage`|`Source`|`Provider`} and **Destination** {`Sink`|`Target`}.
**Transfer** is a **LOGICAL** data transfer service. The minimum unit of data is a logical **ROW** (object). Between **source** and **target** we communicate via [ChangeItem](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/abstract/changeset.go?rev=r10623357#L57)-s.
Those items batched and we may apply stateless [Transformations](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/transformer/?rev=r10626635).
Overall this pipeline called [Transfer](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/server/model_transfer.go?rev=420c3cb117#L31)

We could compose our primitive to create 2 main different types of connection

1. {`Storage`|`Provider`} + {`Sink`|`Target`} = `Snapshot`
2. {`Source`} + {`Sink`|`Target`} = `Replication`

These 2 directions are conceptually different and have different requirements for specific storages.
Snapshot and Replication threads can follow each other.
Event channels are conceptually unaware of the base types they bind.
We mainly build cross system data connection (or as we called them **Hetero** replications), therefore we are not adding any nitpicking for them (type fit or schema adjustment).
But for connection between same type of storages to improve accuracy, the system can tell `Source`|`Storage`|`Sinks` if they are homogeneous (or simply **Homo** replication), and do some adjustments and fine-tuning.
Apart from this cross db-type connections should **NOT** know of what type of storage on apart side.

## Storage / SnapshotProvider

Large-block reading primitive from data. The final stream of events of one type is the insertion of a row. It can give different levels of read consistency guarantees, depending on the depth of integration into a particular database.

![snapshot image](https://double.cloud/assets/blog/articles/transferring-data-1.png)

### ROW level Gurantee

At the most primitive storage level, it is enough to implement the reading of all logical lines from the source to work. In this case, the unit of consistency is the string itself. Example - if we say that one line is one file on disk, then reading the directory gives a guarantee of consistency within one specific file.


### Table level Gurantee

Rows are logically grouped into groups of homogeneous rows, usually tables. If the source is able to read a consistent snapshot of the rows of one table, then we can guarantee that the data is consistent at the entire table level. From the point of view of the contract, consistency at the table / row level is indistinguishable for us.

### Whole Storage

It can be arranged if we can take a consistent snapshot and reuse it to read several tables (for example, reading in one transaction sequentially or having a transaction pool with one database state).

### Point of replication (Replication Slot)

If the source can atomically take a snapshot / snapshot mark for reading and a mark for future replication, we can implement a consistent transition between the snapshot and the replica.

### Summary

From a contractual point of view, consistency at the table/row level is **indistinguishable** for us. We have no clear signs to clearly define with what level of assurance we have read the data from the source.

## Source / ReplicationProvider

A streaming primitive. An endless stream of CRUD events line by line. In logical replication, **conceptually** there are only 3 types of events - create / edit / delete. For editing and deleting, we need to somehow identify the object with which we operate, so to support such events, we expect the source itself to be able to give them.

![tx-bounds](https://double.cloud/assets/blog/articles/transferring-data-3.png)

For some storages such events can be grouped into transactions.

![replication-lag](https://double.cloud/assets/blog/articles/transferring-data-4.png)

Once we start replication process we apply this stream of actions to target and try to minimize our data-lag between source database and target.

At the replication source level, we maintain different levels of consistency:

### Row

This is the most basic mechanism, if the source does not link strings to each other, then there is a guarantee only at the string level. An example of MongoDB in FullDocument mode, each event in the source is one row living in its own timeline. Events with this level of assurance do not have a transaction tag and logical source time (LSN) **or** not in a strict order.

### Table

If the rows begin to live in a single timeline - we can give consistency at the table level, applying the entire stream of events in the same order as we received them gives us a consistent slice of the table **Eventually**. Events with this level of guarantee do not have a transaction stamp in them, but contain a source logical timestamp (LSN) **and** a strict order.

### Transaction

If the rows live in a single timeline and are attributed with transaction labels, as well as linearized in the transaction log (that is, there is a guarantee that all changes in one transaction are continuous and the transactions themselves are logically ordered) - we can give consistency at the table and transaction levels. Applying the entire stream of events in the same order with the same (or larger) batches of transactions, we will get a consistent slice of the table from the source at **any** moment in time.

## Sink / Target

Each of our Targets is a simple thing that can consume a stream of events; at its level, the target can both support source guarantees and weaken them.

### Primitive

At the most basic level, the target simply writes everything that comes in (the classic example is the / fs / s3 queue), at this level we do not guarantee anything other than the very fact of writing everything that comes in (while the records may be duplicated).

### Unique Key deduplication

The Target can de-duplicate the row by the primary key, in which case we give an additional guarantee - there will be no key duplicates in the target.

### Logical clock deduplication

If the Target can write to 2 tables in single transaction, we can transactional store the source logical timestamp in separate table and discard already written rows. In this case, there will be no duplicates in the targets, including in lines without keys.

### Transaction boundaries

If the receiver can hold transactions for an arbitrarily long time and apply transactions of an arbitrary size, we can implement saving transaction boundaries on writes. In this case, the sink will receive rows in the same or larger transactions, which will give an exact cut of the source at **any** point in time.

## Summary

For maximum guarantees (exact slice of the source at **any** point in time) both the source and the destination should give maximum guarantee between themselves.

For current storages, we have approximately the following matrix:

| Storage Type | S/Row |S/Table|S/DB|S/Slot|R/Row|R/Table|R/TX|T/Rows|T/Keys|T/LSN|T/TX|
|:-------------|:------|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| PG           | \+    |\+|\+|\+|\+|\+|\+|\+|\+|\+|\+|
| Mysql        | \+    |\+|\+||\+|\+|\+|\+|\+|\+|\+|
| Mongodb      | \+    ||||\+|||\+|\+|||
| Clickhouse   | \+    |||||||\+||\+||
| Greenplum    | \+    |\+|\+|||||\+|\+|\+|\+|
| Oracle       | \+    |\+|\+|\+|\+|\+|\+|\+|\+|\+|\+|
| YDB          | \+    |\+||||||\+|\+|||
| YT           | \+    |\+||||||\+|\+|\+||
| Airbyte      | \+    |\+/-||||\+/-||\+|\+/-|||
| Kafka        | \+    ||||\+|||\+||||
| EventHub     | \+    ||||\+|||\+||||
| LogBroker    | \+    ||||\+|||\+||\+||

# Quick start

1. Clone [arcadia](https://docs.yandex-team.ru/devtools/intro/quick-start-guide)
2. Setup your [IDE](https://docs.yandex-team.ru/arcadia-golang/getting-started)
    ```shell
      ya ide goland transfer_manager/go -P ~/GolandProjects/arcadia --with-yoimports
    ```
3. Generate proto-gen result of API proto files and dependencies
    ```shell
       ya make -DDATA_TRANSFER_DISABLE_CGO -DCGO_ENABLED=0 ./transfer_manager/go --add-result .go --replace-result -k -j 16
    ```
4. Build control-plane cmd:
    ```shell
      ya make transfer_manager/go/cmd/control-plane
    ```
    Or for a debug build for debugging with Delve, build like this:
    ```shell
       ya make transfer_manager/go/cmd/control-plane -DGO_COMPILE_FLAGS='-N -l'
    ```
    If for some reason you need to build native `go build`, then build with the `CGO_ENABLED=0` env variable:
    ```shell
       cd transfer_manager/go/cmd/control-plane
       CGO_ENABLED=0 go build
    ```
    If you need to build a single binary that includes both control-plane and data-plane in order to run operations with a local runtime, then you need to add the -DDATA_TRANSFER_ALL_IN_ONE_BINARY flag or the all_in_one_binary tag:
    ```shell
       cd transfer_manager/go/cmd/control-plane
       # with ya make
       ya make -j16 -DDATA_TRANSFER_DISABLE_CGO -DDATA_TRANSFER_ALL_IN_ONE_BINARY
       # or with go build
       go build -tags all_in_one_binary .
    ```
5. Prepare your local config.
   The development configs in the devconfigs directory are for local development. You can specify the database schema in the config file by changing the value of the `db_schema` field - then you will not intersect with other developers in the database.
6. Start `control-plane` binary. For `all_in_one_binary` you must provide both `--control-plane` and `--data-plane` config, for example:
    ```shell
       ./control-plane --controlplane-config-file cpconfig.yaml --dataplane-config-file dpconfig.yaml
    ```
    The developer may configure to local launch of both **tasks** on **replications** (activation, deactivation of transfers, etc.) locally (on your local machine). To do this, you pass both configs on the command line for both control-plane and data-plane parts.
    Production installations run tasks asynchronously in external runtimes (like separate EC2 machine, K8S-jobs, YT vanilla operation or compute cloud virtual machines in Yandex.cloud). To run tasks in an external runtime, you need to change the values of the options `sync_tasks`
    and `local_replication` to **false**. In this case, it is not necessary to pass `--dataplane-config-file` to the CLI, because you are running `control-plane`-cli in control plane only mode.
    In general, you can run with a testing or pre-prod config, in this case you will not break anything and everything will work for you out of the box if you have correct auth profile for YC/YCP/ENV in the environment and if you have access to all the secrets in the secret box. For example, you can run like this:
    ```
    ./cdc_server --controlplane-config-file=$ARCAIDA_ROOT/transfer_manager/go/pkg/config/controlplane/devconfigs/aws.yaml
    ```

### Notes for GoLand
1. Specify tag `all_in_one_binary`. It's better in GoLand -> Preferences -> Go -> Build Tags & Vendoring so that GoLand also understands which sources to use.
2. Enable [AllInOneBinary](https://a.yandex-team.ru/arc_vcs/transfer_manager/go/pkg/config/config.go?rev=r8800862#L46)


