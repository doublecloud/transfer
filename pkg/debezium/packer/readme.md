## Packers

Packer - convert kafka schema & payload into debezium messages

Packers should be thread-safe.

We have 3 functional packers:

- PackerIncludeSchema - packer who includes schema - default debezium behaviour
- PackerSkipSchema - packer who skips schema - behaviour like debezium 'schema.enable:false'
- PackerSchemaRegistry - packer who uses confluent schema registry (json) - confluent SR, converting kafka schema into confluent schema & resolving schema into schemaID & writes in confluent wire format

And two auxiliary packers

- PackerCacheFinalSchema - packer who caches final schema (it's actually pattern 'decorator') - keeping tableSchemaAsString->finalSchemaBytes cache, and not to calls 'BuildFinalSchema' 2nd time.
- lightning_cache/PackerLightningCache - packer who exists only in one batch serialization (it's actually pattern 'decorator'), cacher either schemaID or finalSchema - in dependency of base packer.
