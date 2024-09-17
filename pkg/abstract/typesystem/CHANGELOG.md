# List of typesystem fallbacks

* `2` to `1`:
  * [pkg/dataagent/ch/fallback_timestamp_as_datetime.go](../../../pkg/dataagent/ch/fallback_timestamp_as_datetime.go)
  * [pkg/dataagent/pg/fallback_date_as_string.go](../../../pkg/dataagent/pg/fallback_date_as_string.go)
* `3` to `2`:
  * [pkg/dataagent/pg/fallback_not_null_as_null.go](../../../pkg/dataagent/pg/fallback_not_null_as_null.go)
* `4` to `3`:
  * [pkg/dataagent/pg/fallback_timestamp_utc.go](../../../pkg/dataagent/pg/fallback_timestamp_utc.go)
* `5` to `4`:
  * [pkg/providers/kafka/fallback_generic_parser_timestamp.go](../../../pkg/providers/kafka/fallback_generic_parser_timestamp.go)
  * [pkg/providers/logbroker/fallback_generic_parser_timestamp.go](../../../pkg/providers/logbroker/fallback_generic_parser_timestamp.go)
  * [pkg/providers/yds/fallback_generic_parser_timestamp.go](../../../pkg/providers/yds/fallback_generic_parser_timestamp.go)
* `6` to `5`:
  * [pkg/dataagent/pg/fallback_bit_as_bytes.go](../../../pkg/dataagent/pg/fallback_bit_as_bytes.go)
* `7` to `6`:
    * [pkg/providers/mongo/fallback_dvalue_json_repack.go](../../../pkg/providers/mongo/fallback_dvalue_json_repack.go)
* `8` to `7`:
    * [pkg/providers/yt/fallback_bytes_as_string_go_type.go](../../../pkg/providers/yt/fallback_bytes_as_string_go_type.go)
* `9` to `8`:
    * [pkg/providers/ydb/fallback_date_and_datetime_as_timestamp.go](../../../pkg/providers/ydb/fallback_date_and_datetime_as_timestamp.go)
