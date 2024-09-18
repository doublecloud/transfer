package elastic

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/tests/helpers/utils"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/stretchr/testify/require"
)

func TestFixDataTypesWithSampleData(t *testing.T) {
	storage, err := NewStorage(&ElasticSearchSource{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), ElasticSearch)
	require.NoError(t, err)
	searchFuncStub := func(o ...func(*esapi.SearchRequest)) (*esapi.Response, error) {
		readCloser := utils.NewTestReadCloser()
		readCloser.Add([]byte(`{"hits":{"hits":[{"_id":"my_id", "_source": {"k": null}}]}}`))
		return &esapi.Response{
			StatusCode: 200,
			Header:     nil,
			Body:       readCloser,
		}, nil
	}
	storage.Client.Search = searchFuncStub

	schemaDescription := &SchemaDescription{
		Columns: []abstract.ColSchema{
			{ColumnName: "k"},
		},
		ColumnsNames: []string{"k"},
	}

	err = storage.fixDataTypesWithSampleData("", schemaDescription)
	require.NoError(t, err)
}
