# Serializer

The serializer package defines the ways in which we can expose our events to the user. Currently the following formats are available in this package:

* debezium

* json (aka simpleJSON) - we haven't yet fully discussed

* mirror

* native

When we need to expose our events to the user - mostly when we ship data in the queue (`lb/yds/kafka/eventhub`).

Here are the current arrangements (in abstract, more details in the following chapters):

* **debezium** - for `pg-source`, we can expose debezium format to any dst queue to anyone at any time - internally and externally.

* **json (aka simpleJSON)** - we haven't yet agreed on the final variant of disclosure - what restrictions are needed to disclose data in this format to the user.

* **mirror** - this is an internal representation of the transfer of raw data between queues (essentially a container). This is our service thing - we don't disclose the container format to anyone.

* **native** is our change items (aka abstract1) serialized in JSON. We don't disclose it to anyone.

The best recommendation is to talk to @vag-ekaterina and @timmyb32r before using this package.

## debezium

The **Debezium** format is covered by tests so far only for `pg-source` - so, for `pg-source`, we disclose to anyone and everyone. For other sources, the rule is:

* If the source is supported by **debesium** (and this is primarily `pg/mysql/oracle/mongodb`) - disclose after we cover it with tests for full compliance with debesium.

* If the source is not supported by **debesium** and support is not expected in the coming years (`yt/ydb`) - determine the data serialization format (preferably in the spirit of debesium), cover with tests and disclose.

## json (aka simpleJSON)`

This is a very simple format - for example a line with columns `key,value` with values `1`, `blablabla` the example view is:

```json
{"key":1, "value": "blablabla"}
```

Its disadvantages:

* There can only be `INSERT`

* There is nothing about data types here - only what json allows.

* It doesn't even have a `table_name`

* No meta-information at all

Why this might be useful:

* For airbyte sources

* For refills of a single table

We're not revealing this format to the user yet, because we want to clearly define a number of points (which will make backwards compatibility easier for us):

* Do we want to disallow this on replication at all, if the only kinds supported are inserts? (refills are snapshots)

* Do we want to use it for non-airbyte sources?

* Do we want to use it for debezium wherever it's available?

* Do we want to open simpleJSON for mysql-like cases? When debezium already supports it, we don't, so we can't guarantee the values representation.

* We need to finalize the name - debezium is also a json, our working name is `simpleJSON`.

## mirror

This is the internal representation of the raw data transfer between the queues (essentially a container).

The internal structure of the format should not be revealed to any users - users should just see the same messaging in the dst-queue as they took from the src-queue.

Messages of this format should be serialized only in tests.

That is why, by the way, we can change this format as we want - because in production it exists only in memory and is not serialized.

## native

These are our change items (aka abstract1), serialized in JSON.

We haven't published this format, and it would be great if it stayed that way.

There are two users inside (with `kafka` and `logbroker` receivers) that are mapped to our native format.

There are also some users inside with YT-receiver, who have the option to ship a wal - they also see our native format in yt.

To existing users - give best effort to backward compatibility (considering that we did not disclose the format and did not give guarantees - these users are evil cobblers themselves), however, do not disclose to new users.

## Known botches

* Currently, we have two different mirror "protocols" - so in `mirror_serializer` we have `Serialize/SerializeBatch` and `SerializeLB`. Let's fix it in TM-3855.

* Currently, we also have Native writing into logbroker an array of change items per one message, and into kafka one change item per one message. We will be able to get rid of it as soon as rent moves to debezium.

## SaveTxOrder

The **SaveTxOrder** setting is supported in all serializers (except mirror, because it makes no sense there).

By default the **SaveTxOrder** setting is `disabled`.

The setting is responsible for serialized stream split.

If the setting is `disabled` - event stream is split by table names which can be further processed in parallel.

This has its advantages - for example, per-table data can be written in parallel, increasing throughput.

You can also write data from each table to your own storage (e.g. a special topic), and those who need to handle a thread of one particular table will be able to do it most efficiently.

But this approach makes it much more difficult to deduct per-transaction data if more than 1 table has been changed in one transaction.

Accordingly, disabling this setting (default behavior) is effective for concurrency and working with tables independently.

If the setting is `enabled`, the event stream remains in the same order in which it came from the source. Therefore you can't talk about any concurrency - everything starts working in one thread, and e.g. lb-receiver will write the whole thread to one partition of one topic. But this mode of work allows to easily subtract all events of one transaction - because they will execute sequentially.

For example, let's consider the serializer in the logger-receiver.

* If the setting is `disabled` - we can write to many topic per table, as well as to many partitions of one topic. Each table will have its own sourceID (from table name), and it will guarantee that all data from the same table are in the same partition of the same topic.

* If this setting is `enabled`, we will only write to one partition of one topic - even if there are many tables. But all data will be grouped by transactions.

From the interface perspective, this setting is in the target endpoints (lb/kafka) in advanced options.
