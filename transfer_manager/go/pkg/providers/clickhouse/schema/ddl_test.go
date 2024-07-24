package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDistributedDDL(t *testing.T) {
	ddl := "CREATE TABLE logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	require.False(t, IsDistributedDDL(ddl))

	ddl = "CREATE TABLE logs.test7 ON CLUSTER (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	require.True(t, IsDistributedDDL(ddl))

	ddl = "CREATE TABLE logs.test7 on  cluster (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	require.True(t, IsDistributedDDL(ddl))
}

func TestReplaceCluster(t *testing.T) {
	ddl := "CREATE TABLE logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL := ReplaceCluster(ddl, "{cluster}")
	require.Equal(t, changedDDL, ddl)

	ddl = "CREATE TABLE logs.test7 ON CLUSTER `abcdef` (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL = ReplaceCluster(ddl, "{cluster}")
	require.Equal(t,
		"CREATE TABLE logs.test7 ON CLUSTER `{cluster}` (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192",
		changedDDL,
	)

	ddl = "CREATE TABLE logs.test7 on  cluster abcdef (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL = ReplaceCluster(ddl, "{cluster}")
	require.Equal(t,
		"CREATE TABLE logs.test7 ON CLUSTER `{cluster}` (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192",
		changedDDL,
	)
}

func TestSetReplicatedEngine(t *testing.T) {
	ddl := "CREATE TABLE logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	_, err := SetReplicatedEngine(ddl, "MergeTree", "logs", "test7")
	require.Error(t, err)

	changedDDL, err := SetReplicatedEngine(ddl, "ReplicatedMergeTree", "logs", "test7")
	require.NoError(t, err)
	require.Equal(t, ddl, changedDDL)

	ddl = "CREATE TABLE default.attributes (`id` UInt32, `event_date` Date, `orders_count` UInt32, `rating` UInt8) ENGINE = MergeTree ORDER BY event_date SETTINGS index_granularity = 8192"
	changedDDL, err = SetReplicatedEngine(ddl, "MergeTree", "default", "attributes")
	require.NoError(t, err)
	require.Equal(t,
		"CREATE TABLE default.attributes (`id` UInt32, `event_date` Date, `orders_count` UInt32, `rating` UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default.attributes_cdc', '{replica}') ORDER BY event_date SETTINGS index_granularity = 8192",
		changedDDL,
	)

	ddl = "CREATE TABLE default.attributes (`id` UInt32, `event_date` Date, `orders_count` UInt32, `rating` UInt8) ENGINE = MergeTree() ORDER BY event_date SETTINGS index_granularity = 8192"
	changedDDL, err = SetReplicatedEngine(ddl, "MergeTree", "default", "attributes")
	require.NoError(t, err)
	require.Equal(t,
		"CREATE TABLE default.attributes (`id` UInt32, `event_date` Date, `orders_count` UInt32, `rating` UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default.attributes_cdc', '{replica}') ORDER BY event_date SETTINGS index_granularity = 8192",
		changedDDL,
	)

	ddl = "CREATE TABLE default.search_conversions ( `search_uuid` String, `search_id` Int32, `uuid` String, `client_id` UInt32, `search_date` Date, `updated_at` UInt32, `search_at` UInt32, `pageview_at` Nullable(UInt32), INDEX hp pageview_at TYPE minmax GRANULARITY 1) ENGINE = ReplacingMergeTree PARTITION BY search_date ORDER BY (uuid, search_id) SETTINGS index_granularity = 8192"
	changedDDL, err = SetReplicatedEngine(ddl, "ReplacingMergeTree", "default", "search_conversions")
	require.NoError(t, err)
	require.Equal(t,
		"CREATE TABLE default.search_conversions ( `search_uuid` String, `search_id` Int32, `uuid` String, `client_id` UInt32, `search_date` Date, `updated_at` UInt32, `search_at` UInt32, `pageview_at` Nullable(UInt32), INDEX hp pageview_at TYPE minmax GRANULARITY 1) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/default.search_conversions_cdc', '{replica}') PARTITION BY search_date ORDER BY (uuid, search_id) SETTINGS index_granularity = 8192",
		changedDDL,
	)
}

func TestSetIfNotExists(t *testing.T) {
	ddl := "CREATE TABLE logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL := SetIfNotExists(ddl)
	require.Equal(t,
		"CREATE TABLE IF NOT EXISTS logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192",
		changedDDL,
	)

	ddl = "CREATE TABLE IF NOT EXISTS logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL = SetIfNotExists(ddl)
	require.Equal(t, changedDDL, ddl)

	ddl = "CREATE MATERIALIZED VIEW logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL = SetIfNotExists(ddl)
	require.Equal(t,
		"CREATE MATERIALIZED VIEW IF NOT EXISTS logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192",
		changedDDL,
	)
}

func TestMakeDistributedDDL(t *testing.T) {
	ddl := "CREATE TABLE logs.test7 (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL := MakeDistributedDDL(ddl, "{cluster}")
	require.Equal(t,
		"CREATE TABLE logs.test7  ON CLUSTER `{cluster}` (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192",
		changedDDL,
	)

	ddl = "CREATE TABLE logs.test7 ON CLUSTER abcd (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192"
	changedDDL = MakeDistributedDDL(ddl, "{cluster}")
	require.Equal(t,
		"CREATE TABLE logs.test7 ON CLUSTER `{cluster}` (`id` String, `counter` Int32, `vals` Array(String) CODEC(LZ4HC(9))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs.test7_cdc', '{replica}') ORDER BY id SETTINGS index_granularity = 8192",
		changedDDL,
	)
}

func TestComplexDDL(t *testing.T) {
	ddl := "CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1-be74-91ee102b90b1' ON CLUSTER chclpinrb4126vagc689\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192"
	require.Equal(t,
		"CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1-be74-91ee102b90b1' ON CLUSTER `{cluster}`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192",
		MakeDistributedDDL(ddl, "{cluster}"))
}

func TestSpacedDDL(t *testing.T) {
	ddl := "CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1-be74-91ee102b90b1' ON CLUSTER `chclpinrb4126va gc689`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192"
	require.Equal(t,
		"CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1-be74-91ee102b90b1' ON CLUSTER `{cluster}`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192",
		MakeDistributedDDL(ddl, "{cluster}"))

	ddl = "CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1- be74-91ee102b90b1' ON CLUSTER `chclpinrb4126va gc689`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192"
	require.Equal(t,
		"CREATE TABLE IF NOT EXISTS research.all_serp_competitor UUID 'fe7491ee-102b-40b1- be74-91ee102b90b1' ON CLUSTER `{cluster}`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192",
		MakeDistributedDDL(ddl, "{cluster}"))

	ddl = "CREATE TABLE IF NOT EXISTS research.all_serp_competitor ON CLUSTER `chclpinrb4126va gc689`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192"
	require.Equal(t,
		"CREATE TABLE IF NOT EXISTS research.all_serp_competitor ON CLUSTER `{cluster}`\n(\n    `timestamp` DateTime DEFAULT now(),\n    `keyword` String,\n    `country` String,\n    `lang` String,\n    `organicRank` UInt8,\n    `search_volume` UInt64,\n    `url` String,\n    `subDomain` String,\n    `domain` String,\n    `path` String,\n    `etv` UInt64,\n    `clickstream_etv` Float64,\n    `related` Array(String),\n    `estimated_traffic` UInt64 DEFAULT CAST(multiIf(organicRank = 1, search_volume * 0.3197, organicRank = 2, search_volume * 0.1551, organicRank = 3, search_volume * 0.0932, organicRank = 4, search_volume * 0.0593, organicRank = 5, search_volume * 0.041, organicRank = 6, search_volume * 0.029, organicRank = 7, search_volume * 0.0213, organicRank = 8, search_volume * 0.0163, organicRank = 9, search_volume * 0.0131, organicRank = 10, search_volume * 0.0108, organicRank = 11, search_volume * 0.01, organicRank = 12, search_volume * 0.0112, organicRank = 13, search_volume * 0.0124, organicRank = 14, search_volume * 0.0114, organicRank = 15, search_volume * 0.0103, organicRank = 16, search_volume * 0.0099, organicRank = 17, search_volume * 0.0094, organicRank = 18, search_volume * 0.0081, organicRank = 19, search_volume * 0.0076, organicRank = 20, search_volume * 0.0067, 0.), 'Int64'),\n    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 8192,\n    INDEX domain_bf domain TYPE bloom_filter GRANULARITY 65536,\n    PROJECTION order_by_keyword__domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY keyword\n    ),\n    PROJECTION order_by_organic_rank__keyword_domain_country_lang_organic_rank\n    (\n        SELECT\n            keyword,\n            domain,\n            country,\n            lang,\n            organicRank\n        ORDER BY organicRank\n    )\n)\nENGINE = MergeTree\nORDER BY (country, domain)\nSETTINGS index_granularity = 8192",
		MakeDistributedDDL(ddl, "{cluster}"))

}
