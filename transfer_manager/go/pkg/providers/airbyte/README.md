## Airbyte provider

This is a bridge between native transfer and airbyte connector.

We support source airbyte [connectors](https://docs.airbyte.com/category/sources)

### How to add new airbyte connector

#### 1. Add proto-model

Each airbyte provider has own config. To get such config you can run:

```shell
docker run airbyte/source-snowflake:0.1.31 spec
```

This would output airbyte [spec](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#spec) for a connector.
Output may contains also logs, but some line should have type `SPEC`. Something like this:

```shell
{"type":"LOG","log":{"level":"INFO","message":"INFO i.a.i.b.IntegrationRunner(runInternal):107 Integration config: IntegrationConfig{command=SPEC, configPath='null', catalogPath='null', statePath='null'}"}}
{"type":"SPEC","spec":{"documentationUrl":"https://docs.airbyte.com/integrations/sources/snowflake","connectionSpecification":{"$schema":"http://json-schema.org/draft-07/schema#","title":"Snowflake Source Spec","type":"object","required":["host","role","warehouse","database"],"properties":{"credentials":{"title":"Authorization Method","type":"object","oneOf":[{"type":"object","title":"OAuth2.0","order":0,"required":["client_id","client_secret","auth_type"],"properties":{"auth_type":{"type":"string","const":"OAuth","order":0},"client_id":{"type":"string","title":"Client ID","description":"The Client ID of your Snowflake developer application.","airbyte_secret":true,"order":1},"client_secret":{"type":"string","title":"Client Secret","description":"The Client Secret of your Snowflake developer application.","airbyte_secret":true,"order":2},"access_token":{"type":"string","title":"Access Token","description":"Access Token for making authenticated requests.","airbyte_secret":true,"order":3},"refresh_token":{"type":"string","title":"Refresh Token","description":"Refresh Token for making authenticated requests.","airbyte_secret":true,"order":4}}},{"title":"Username and Password","type":"object","required":["username","password","auth_type"],"order":1,"properties":{"auth_type":{"type":"string","const":"username/password","order":0},"username":{"description":"The username you created to allow Airbyte to access the database.","examples":["AIRBYTE_USER"],"type":"string","title":"Username","order":1},"password":{"description":"The password associated with the username.","type":"string","airbyte_secret":true,"title":"Password","order":2}}}],"order":0},"host":{"description":"The host domain of the snowflake instance (must include the account, region, cloud environment, and end with snowflakecomputing.com).","examples":["accountname.us-east-2.aws.snowflakecomputing.com"],"type":"string","title":"Account Name","order":1},"role":{"description":"The role you created for Airbyte to access Snowflake.","examples":["AIRBYTE_ROLE"],"type":"string","title":"Role","order":2},"warehouse":{"description":"The warehouse you created for Airbyte to access data.","examples":["AIRBYTE_WAREHOUSE"],"type":"string","title":"Warehouse","order":3},"database":{"description":"The database you created for Airbyte to access data.","examples":["AIRBYTE_DATABASE"],"type":"string","title":"Database","order":4},"schema":{"description":"The source Snowflake schema tables. Leave empty to access tables from multiple schemas.","examples":["AIRBYTE_SCHEMA"],"type":"string","title":"Schema","order":5},"jdbc_url_params":{"description":"Additional properties to pass to the JDBC URL string when connecting to the database formatted as 'key=value' pairs separated by the symbol '&'. (example: key1=value1&key2=value2&key3=value3).","title":"JDBC URL Params","type":"string","order":6}}},"supportsNormalization":false,"supportsDBT":false,"supported_destination_sync_modes":[],"advanced_auth":{"auth_flow_type":"oauth2.0","predicate_key":["credentials","auth_type"],"predicate_value":"OAuth","oauth_config_specification":{"oauth_user_input_from_connector_config_specification":{"type":"object","properties":{"host":{"type":"string","path_in_connector_config":["host"]},"role":{"type":"string","path_in_connector_config":["role"]}}},"complete_oauth_output_specification":{"type":"object","properties":{"access_token":{"type":"string","path_in_connector_config":["credentials","access_token"]},"refresh_token":{"type":"string","path_in_connector_config":["credentials","refresh_token"]}}},"complete_oauth_server_input_specification":{"type":"object","properties":{"client_id":{"type":"string"},"client_secret":{"type":"string"}}},"complete_oauth_server_output_specification":{"type":"object","properties":{"client_id":{"type":"string","path_in_connector_config":["credentials","client_id"]},"client_secret":{"type":"string","path_in_connector_config":["credentials","client_secret"]}}}}}}}
{"type":"LOG","log":{"level":"INFO","message":"INFO i.a.i.b.IntegrationRunner(runInternal):182 Completed integration: io.airbyte.integrations.source.snowflake.SnowflakeSource"}}
```

Spec line contains `connectionSpecification` key with JSON schema model, this model can be a base for our proto model.

This specification can be baseline for new proto, for example we can use some semi-automatic [tools](https://transform.tools/json-schema-to-protobuf) to convert json-schema into proto.

As example you can refer to exist [protos](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/endpoint/airbyte/?rev=r10953642)

#### 2. Adding docker image and provider type

Each user facing provider has own value of [API-Providers enum](https://a.yandex-team.ru/arcadia/transfer_manager/go/proto/api/endpoint.proto?rev=r10968706#L74). If there is no provider in enum - you need to add new one.

Beside that this enum should be added to [Known Airbyte Providers](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/providers/airbyte/provider_model.go?rev=r11073103#L29) list.

To create linkage between API-enum into specific Airbyte provider-code we need to add mappings:

1. Between Provider Enum and proto-model [here](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/providers/airbyte/provider_model.go?rev=r11073103#L303)
1. Between Provider Enum and Airbyte-docker image [here](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/providers/airbyte/provider_model.go?rev=r11073103#L282)

By default, we map proto message into json message as is with standard proto-json mapper, but for some cases (for example one-of fields) we should add extra mapping code like we do [here](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/providers/airbyte/provider_model.go?rev=r11073103#L351).

#### 3. Enable new provider

For managing providers across installation we use [grants](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/grants/?rev=r11099251) mechanism, so as last step we should add the new airbyte grant like we do [here](https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/grants/grants.go?rev=r11099251#L67).

In an initial phase its recommended to enable the new provider in preview mode until a certain stability can be proven.
```
	NewAirbyteGrant(api.EndpointType_SNOWFLAKE, "Snowflake",
		AWS().Preview(),
	)
```

#### 4. Optional e2e test

We have a fat pre-commit test for the airbyte connectors which is run by a [special script `go/tests/e2e/run_teamcity_docker_tests.sh`](../../../tests/e2e/run_teamcity_docker_tests.sh), so if you have a stable instance that can be used as CI-source you can add a new test like it's done [in `s3csv`](../../../tests/e2e/airbyte2ch/s3csv/check_db_test.go) or another test in that directory.

#### Example PR adding Airbyte Snowflake connector

For a full example adding a new airbyte connectory you can have a look at this [PR](https://a.yandex-team.ru/review/3653374/files/1)