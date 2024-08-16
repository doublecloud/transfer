package functions

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/stretchr/testify/require"
)

func TestRedirectsForbidden(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1/test", http.StatusTemporaryRedirect)
	}))
	opts := &server.DataTransformOptions{CloudFunction: "whatever"}
	baseURL := "http://" + testServer.Listener.Addr().String()
	executor, err := NewExecutor(opts, baseURL, YDS, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	_, err = executor.Do([]abstract.ChangeItem{{
		Kind:         abstract.InsertKind,
		Schema:       "s",
		Table:        "t",
		ColumnNames:  abstract.RawDataSchema.ColumnNames(),
		ColumnValues: []any{"topic", uint32(0), uint64(0), time.Time{}, "kek"},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no redirects are allowed")
}
