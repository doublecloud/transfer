# Clickhouse Destination

Clickhouse destination support 2 mode:

1. Replication: All tables created as Replacing Merge Tree family
2. Snapshot Only: All tables created just as Merge Tree family

If cluster is replicated - then tables would be generated as Replicated Merge Tree family.

Primary key from source by default generates order by clause for target DDL-s
