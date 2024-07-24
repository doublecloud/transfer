package storage

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/s3"
	"github.com/stretchr/testify/require"
)

func TestShardWithBlob(t *testing.T) {
	testCasePath := "yellow_taxi"
	cfg := s3.PrepareCfg(t, "blobiki_bobiki", server.ParsingFormatPARQUET)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	tid := *abstract.NewTableID(cfg.TableNamespace, cfg.TableName)
	t.Run("single blob", func(t *testing.T) {
		cfg.PathPattern = "*2023*" // only include 2023 year.
		storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
		require.NoError(t, err)
		require.Equal(t, len(files), 2)
	})
	t.Run("all", func(t *testing.T) {
		cfg.PathPattern = "*" // all files
		storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
		require.NoError(t, err)
		require.Equal(t, len(files), 4)
	})
	t.Run("or case", func(t *testing.T) {
		cfg.PathPattern = "*2023*|*2022-12*" // 2023 and one month of 2022
		storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		files, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
		require.NoError(t, err)
		require.Equal(t, len(files), 3)
	})
}
