package engine

import (
	"encoding/json"
	"strings"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/generic"
	jsonparser "github.com/doublecloud/transfer/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type AuditTrailsV1ParserImpl struct {
	useElasticSchema bool
	parser           parsers.Parser
}

func (p *AuditTrailsV1ParserImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	if p.useElasticSchema {
		lines := strings.Split(string(msg.Value), "\n")
		result := make([]abstract.ChangeItem, 0)
		for i, line := range lines {
			finalLine, err := ExecProgram(line)
			if err != nil {
				result = append(result, generic.NewUnparsed(partition, partition.Topic, line, err.Error(), i, msg.Offset, msg.WriteTime))
			} else {
				// ok, transformed
				var myMap map[string]interface{}
				_ = json.Unmarshal([]byte(finalLine), &myMap)

				changeItem := abstract.ChangeItem{
					ID:           0,
					LSN:          msg.Offset,
					CommitTime:   uint64(msg.WriteTime.UnixNano()),
					Counter:      0,
					Kind:         abstract.InsertKind,
					Schema:       "",
					Table:        strings.Replace(partition.Topic, "/", "_", -1),
					PartID:       "",
					ColumnNames:  make([]string, 0, len(myMap)),
					ColumnValues: make([]interface{}, 0, len(myMap)),
					TableSchema:  getElasticFields(),
					OldKeys:      abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
					TxID:         "",
					Query:        "",
					Size:         abstract.RawEventSize(uint64(len(line))),
				}

				for k, v := range myMap {
					changeItem.ColumnNames = append(changeItem.ColumnNames, k)
					changeItem.ColumnValues = append(changeItem.ColumnValues, v)
				}
				result = append(result, changeItem)
			}
		}
		return result
	} else {
		return p.parser.Do(msg, partition)
	}
}

func (p *AuditTrailsV1ParserImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, 1000)
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Cluster: "", Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func makeColSchemaWithoutPath(name, type_ string) abstract.ColSchema {
	return makeColSchema(name, "", type_)
}

func makeColSchemaImpl(name, path, type_ string, pkey bool) abstract.ColSchema {
	return abstract.ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         path,
		ColumnName:   name,
		DataType:     type_,
		PrimaryKey:   pkey,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}

func makeColSchema(name, path, type_ string) abstract.ColSchema {
	return makeColSchemaImpl(name, path, type_, false)
}

func makeColSchemaPK(name, path, type_ string) abstract.ColSchema {
	return makeColSchemaImpl(name, path, type_, true)
}

func getNotElasticFields() []abstract.ColSchema {
	result := make([]abstract.ColSchema, 0)
	result = append(result, makeColSchemaPK("event_id", "event_id", "string"))
	result = append(result, makeColSchema("event_source", "event_source", "string"))
	result = append(result, makeColSchema("event_type", "event_type", "string"))
	result = append(result, makeColSchema("event_time", "event_time", "datetime"))
	result = append(result, makeColSchema("authenticated", "authentication.authenticated", "any"))
	result = append(result, makeColSchema("subject_type", "authentication.subject_type", "string"))
	result = append(result, makeColSchema("subject_id", "authentication.subject_id", "string"))
	result = append(result, makeColSchema("subject_name", "authentication.subject_name", "string"))
	result = append(result, makeColSchema("authorized", "authorization.authorized", "any"))
	result = append(result, makeColSchema("remote_address", "request_metadata.remote_address", "string"))
	result = append(result, makeColSchema("user_agent", "request_metadata.user_agent", "string"))
	result = append(result, makeColSchema("request_id", "request_metadata.request_id", "string"))
	result = append(result, makeColSchema("event_status", "event_status", "string"))
	result = append(result, makeColSchema("details", "details", "any"))
	result = append(result, makeColSchema("org_id", "resource_metadata..path[0].resource_id", "string"))
	result = append(result, makeColSchema("org_name", "resource_metadata..path[0].resource_name", "string"))
	result = append(result, makeColSchema("cloud_id", "resource_metadata..path[1].resource_id", "string"))
	result = append(result, makeColSchema("cloud_name", "resource_metadata..path[1].resource_name", "string"))
	result = append(result, makeColSchema("folder_id", "resource_metadata..path[2].resource_id", "string"))
	result = append(result, makeColSchema("folder_name", "resource_metadata..path[2].resource_name", "string"))
	result = append(result, makeColSchema("request_parameters", "request_parameters", "any"))
	result = append(result, makeColSchema("operation_id", "response.operation_id", "string"))
	return result
}

// there are a lot of unused columns, which appear after renaming in `ingest_pipeline.json`
func getElasticFields() *abstract.TableSchema {
	result := make([]abstract.ColSchema, 0)
	result = append(result, makeColSchemaPK("event_id", "", "string"))
	result = append(result, makeColSchemaWithoutPath("@timestamp", "datetime"))
	result = append(result, makeColSchemaWithoutPath("authentication", "any"))
	result = append(result, makeColSchemaWithoutPath("authorization", "any"))
	result = append(result, makeColSchemaWithoutPath("cloud", "any"))
	result = append(result, makeColSchemaWithoutPath("details", "any"))
	result = append(result, makeColSchemaWithoutPath("event", "any"))
	result = append(result, makeColSchemaWithoutPath("event_source", "string"))
	result = append(result, makeColSchemaWithoutPath("event_status", "string"))
	result = append(result, makeColSchemaWithoutPath("event_time", "datetime"))
	result = append(result, makeColSchemaWithoutPath("event_type", "string"))
	result = append(result, makeColSchemaWithoutPath("object_storage", "any"))
	result = append(result, makeColSchemaWithoutPath("request_metadata", "any"))
	result = append(result, makeColSchemaWithoutPath("resource_metadata", "any"))
	result = append(result, makeColSchemaWithoutPath("security_group", "any"))
	result = append(result, makeColSchemaWithoutPath("source", "any"))
	result = append(result, makeColSchemaWithoutPath("user", "any"))
	result = append(result, makeColSchemaWithoutPath("user_agent", "any"))
	result = append(result, makeColSchemaWithoutPath("request_parameters", "any"))
	result = append(result, makeColSchemaWithoutPath("operation_id", "string"))
	return abstract.NewTableSchema(result)
}

func NewAuditTrailsV1ParserImpl(useElasticSchema bool, sniff bool, logger log.Logger, registry *stats.SourceStats) *AuditTrailsV1ParserImpl {
	var parser parsers.Parser
	if useElasticSchema {
		parser = nil
	} else {
		config := &jsonparser.ParserConfigJSONCommon{
			Fields:               getNotElasticFields(),
			SchemaResourceName:   "",
			NullKeysAllowed:      false,
			AddRest:              false,
			AddDedupeKeys:        false,
			UseNumbersInAny:      false,
			UnescapeStringValues: false,
			UnpackBytesBase64:    false,
		}
		parser, _ = jsonparser.NewParserJSON(config, sniff, logger, registry)
	}

	return &AuditTrailsV1ParserImpl{
		useElasticSchema: useElasticSchema,
		parser:           parser,
	}
}
