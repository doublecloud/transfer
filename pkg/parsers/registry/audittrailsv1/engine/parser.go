package engine

import (
	"encoding/json"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/generic"
	jsonparser "github.com/doublecloud/transfer/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var removeNestingNotSupportedError = xerrors.New("Nesting removal is not supported for non-elastic schema")

type AuditTrailsV1ParserImpl struct {
	useElasticSchema       bool
	removeNestingInDetails bool
	parser                 parsers.Parser
}

func (p *AuditTrailsV1ParserImpl) parseLine(line string) (map[string]any, error) {
	finalLine, err := ExecProgram(line)
	if err != nil {
		return nil, xerrors.Errorf("unable to exec pipeline program: %w", err)
	}

	var dict map[string]any
	if err := json.Unmarshal([]byte(finalLine), &dict); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal line: %w", err)
	}

	if p.removeNestingInDetails {
		asAny, found := dict["details"]
		if !found {
			return dict, nil
		}
		details, ok := asAny.(map[string]any)
		if !ok {
			return nil, xerrors.Errorf("details expected to be map[string]any, got %T", details)
		}

		for key, value := range details {
			bytes, err := json.Marshal(value)
			if err != nil {
				return nil, xerrors.Errorf("unable to marshal 'details.%s': %w", key, err)
			}
			details[key] = string(bytes)
		}
		dict["details"] = details
	}

	return dict, nil
}

func (p *AuditTrailsV1ParserImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	if !p.useElasticSchema {
		return p.parser.Do(msg, partition)
	}

	lines := strings.Split(string(msg.Value), "\n")
	result := make([]abstract.ChangeItem, 0, len(lines))

	for i, line := range lines {
		dict, err := p.parseLine(line)
		if err != nil {
			result = append(result, generic.NewUnparsed(
				partition, partition.Topic, line, err.Error(), i, msg.Offset, msg.WriteTime,
			))
			continue
		}

		changeItem := abstract.ChangeItem{
			ID:           0,
			LSN:          msg.Offset,
			CommitTime:   uint64(msg.WriteTime.UnixNano()),
			Counter:      0,
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        strings.Replace(partition.Topic, "/", "_", -1),
			PartID:       "",
			ColumnNames:  make([]string, 0, len(dict)),
			ColumnValues: make([]any, 0, len(dict)),
			TableSchema:  getElasticFields(),
			OldKeys:      abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(uint64(len(line))),
		}

		for k, v := range dict {
			changeItem.ColumnNames = append(changeItem.ColumnNames, k)
			changeItem.ColumnValues = append(changeItem.ColumnValues, v)
		}
		result = append(result, changeItem)
	}
	return result
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
	return []abstract.ColSchema{
		makeColSchemaPK("event_id", "event_id", "string"),
		makeColSchema("event_source", "event_source", "string"),
		makeColSchema("event_type", "event_type", "string"),
		makeColSchema("event_time", "event_time", "datetime"),
		makeColSchema("authenticated", "authentication.authenticated", "any"),
		makeColSchema("subject_type", "authentication.subject_type", "string"),
		makeColSchema("subject_id", "authentication.subject_id", "string"),
		makeColSchema("subject_name", "authentication.subject_name", "string"),
		makeColSchema("authorized", "authorization.authorized", "any"),
		makeColSchema("remote_address", "request_metadata.remote_address", "string"),
		makeColSchema("user_agent", "request_metadata.user_agent", "string"),
		makeColSchema("request_id", "request_metadata.request_id", "string"),
		makeColSchema("event_status", "event_status", "string"),
		makeColSchema("details", "details", "any"),
		makeColSchema("org_id", "resource_metadata..path[0].resource_id", "string"),
		makeColSchema("org_name", "resource_metadata..path[0].resource_name", "string"),
		makeColSchema("cloud_id", "resource_metadata..path[1].resource_id", "string"),
		makeColSchema("cloud_name", "resource_metadata..path[1].resource_name", "string"),
		makeColSchema("folder_id", "resource_metadata..path[2].resource_id", "string"),
		makeColSchema("folder_name", "resource_metadata..path[2].resource_name", "string"),
		makeColSchema("request_parameters", "request_parameters", "any"),
		makeColSchema("operation_id", "response.operation_id", "string"),
	}
}

// there are a lot of unused columns, which appear after renaming in `ingest_pipeline.json`
func getElasticFields() *abstract.TableSchema {
	return abstract.NewTableSchema([]abstract.ColSchema{
		makeColSchemaPK("event_id", "", "string"),
		makeColSchemaWithoutPath("@timestamp", "datetime"),
		makeColSchemaWithoutPath("authentication", "any"),
		makeColSchemaWithoutPath("authorization", "any"),
		makeColSchemaWithoutPath("cloud", "any"),
		makeColSchemaWithoutPath("details", "any"),
		makeColSchemaWithoutPath("event", "any"),
		makeColSchemaWithoutPath("event_source", "string"),
		makeColSchemaWithoutPath("event_status", "string"),
		makeColSchemaWithoutPath("event_time", "datetime"),
		makeColSchemaWithoutPath("event_type", "string"),
		makeColSchemaWithoutPath("object_storage", "any"),
		makeColSchemaWithoutPath("request_metadata", "any"),
		makeColSchemaWithoutPath("resource_metadata", "any"),
		makeColSchemaWithoutPath("security_group", "any"),
		makeColSchemaWithoutPath("source", "any"),
		makeColSchemaWithoutPath("user", "any"),
		makeColSchemaWithoutPath("user_agent", "any"),
		makeColSchemaWithoutPath("request_parameters", "any"),
		makeColSchemaWithoutPath("operation_id", "string"),
	})
}

func NewAuditTrailsV1ParserImpl(
	useElasticSchema, removeNestingInDetails, sniff bool, logger log.Logger, registry *stats.SourceStats,
) (*AuditTrailsV1ParserImpl, error) {

	res := &AuditTrailsV1ParserImpl{
		useElasticSchema:       useElasticSchema,
		removeNestingInDetails: removeNestingInDetails,
		parser:                 nil,
	}

	if !useElasticSchema {
		if removeNestingInDetails {
			return nil, removeNestingNotSupportedError
		}
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
		var err error
		res.parser, err = jsonparser.NewParserJSON(config, sniff, logger, registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create AuditTrails parser: unable to create JSON parser: %s", err)
		}
	}

	return res, nil
}
