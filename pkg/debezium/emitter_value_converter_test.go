package debezium

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	debeziumparameters "github.com/doublecloud/transfer/pkg/debezium/parameters"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func getKV(t *testing.T, changeItem *abstract.ChangeItem, addKeySchema, addValSchema bool) ([]byte, []byte) {
	params := map[string]string{
		debeziumparameters.DatabaseDBName: "public",
		debeziumparameters.TopicPrefix:    "my_topic",
		debeziumparameters.SourceType:     "pg",
	}
	params[debeziumparameters.KeyConverterSchemasEnable] = fmt.Sprintf("%v", addKeySchema)
	params[debeziumparameters.ValueConverterSchemasEnable] = fmt.Sprintf("%v", addValSchema)
	emitter, err := NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))
	return []byte(currDebeziumKV[0].DebeziumKey), []byte(*currDebeziumKV[0].DebeziumVal)
}

func containsSchema(t *testing.T, msg []byte) bool {
	type message struct {
		Schema interface{} `json:"schema"`
	}
	var msgVar message
	err := json.Unmarshal(msg, &msgVar)
	require.NoError(t, err)
	return msgVar.Schema != nil
}

func TestValueConverterOnOff(t *testing.T) {
	changeItem := &abstract.ChangeItem{Kind: abstract.InsertKind}
	k0, v0 := getKV(t, changeItem, true, true)
	require.True(t, containsSchema(t, k0))
	require.True(t, containsSchema(t, v0))
	require.True(t, strings.Contains(string(k0), `"payload"`)) // 'payload' level should present when schema is turned-on
	require.True(t, strings.Contains(string(v0), `"payload"`)) // 'payload' level should present when schema is turned-on
	k1, v1 := getKV(t, changeItem, false, false)
	require.False(t, containsSchema(t, k1))
	require.False(t, containsSchema(t, v1))
	require.False(t, strings.Contains(string(k1), `"payload"`)) // 'payload' level should absent when schema is turned-off
	require.False(t, strings.Contains(string(v1), `"payload"`)) // 'payload' level should absent when schema is turned-off
}

func TestEscapeHTMLMarshaling(t *testing.T) {
	changeItem := &abstract.ChangeItem{
		Kind: abstract.InsertKind,
		ColumnNames: []string{
			"id",
			"value",
		},
		ColumnValues: []interface{}{
			1,
			"<>!@#$%^&*()_",
		},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "value", DataType: ytschema.TypeString.String()},
		})}
	_, payload := getKV(t, changeItem, false, false)
	require.Contains(t, string(payload), `"value":"<>!@#$%^&*()_"`)
}
