package lambda

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/functions"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestLambdaTransformer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := functions.Data{}
		var bodyBytes []byte
		if r.Body != nil {
			bodyBytes, _ = ioutil.ReadAll(r.Body)
		}
		logger.Log.Infof("request into mock server: %v", string(bodyBytes))
		require.NoError(t, json.Unmarshal(bodyBytes, &data))
		for i, r := range data.Records {
			data.Records[i].CDC.ColumnValues[0] = fmt.Sprintf("hello/old/%v/index/%v", r.CDC.ColumnValues[0], i)
			data.Records[i].Result = functions.OK
		}
		js, err := json.Marshal(data)
		require.NoError(t, err)
		w.WriteHeader(200)
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(js)
		require.NoError(t, err)
	}))

	logger.Log.Infof("Using port:%v", ts.Listener.Addr().String())

	transformer := New(Config{
		TableID: abstract.TableID{
			Namespace: "public",
			Name:      "test",
		},
		Options: &server.DataTransformOptions{
			CloudFunction:         "mock-func",
			NumberOfRetries:       1,
			BufferSize:            100 * 1024,
			BufferFlushInterval:   time.Second,
			InvocationTimeout:     time.Minute,
			BackupMode:            server.S3BackupModeNoBackup,
			CloudFunctionsBaseURL: "http://" + ts.Listener.Addr().String(),
		},
	}, logger.Log)
	logger.Log.Infof("description: %v", transformer.Description())

	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "msg", DataType: ytschema.TypeString.String(), PrimaryKey: false},
	})
	builder := helpers.NewChangeItemsBuilder("public", "test", schema)

	changes := builder.Inserts(t, []map[string]any{{
		"msg": "message",
	}})
	schema, err := transformer.ResultSchema(schema)
	require.NoError(t, err)
	logger.Log.Info("schema", log.Any("schena", schema))
	transformed := transformer.Apply(changes)
	require.Empty(t, transformed.Errors)
	require.NotEmpty(t, transformed.Transformed)
	require.Equal(t, "hello/old/message/index/0", transformed.Transformed[0].ColumnValues[0])
}
