package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/library/go/test/canon"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	yt2 "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/yt"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func RecipeYTTarget(path string) yt2.YtDestinationModel {
	ytDestination := yt2.NewYtDestinationV1(yt2.YtDestination{
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Path:          path,
	})
	ytDestination.WithDefaults()
	return ytDestination
}

func SetRecipeYT(dst *yt2.YtDestination) *yt2.YtDestination {
	dst.Cluster = os.Getenv("YT_PROXY")
	dst.CellBundle = "default"
	dst.PrimaryMedium = "default"
	return dst
}

func DumpDynamicYTTable(ytClient yt.Client, tablePath ypath.Path, writer io.Writer) error {
	// Write schema
	schema := new(yson.RawValue)
	if err := ytClient.GetNode(context.Background(), ypath.Path(fmt.Sprintf("%s/@schema", tablePath)), schema, nil); err != nil {
		return xerrors.Errorf("get schema: %w", err)
	}
	if err := yson.NewEncoderWriter(yson.NewWriterConfig(writer, yson.WriterConfig{Format: yson.FormatPretty})).Encode(*schema); err != nil {
		return xerrors.Errorf("encode schema: %w", err)
	}
	if _, err := writer.Write([]byte{'\n'}); err != nil {
		return xerrors.Errorf("write: %w", err)
	}

	reader, err := ytClient.SelectRows(context.Background(), fmt.Sprintf("* from [%s]", tablePath), nil)
	if err != nil {
		return xerrors.Errorf("select rows: %w", err)
	}

	// Write data
	i := 0
	for reader.Next() {
		var value interface{}
		if err := reader.Scan(&value); err != nil {
			return xerrors.Errorf("scan item %d: %w", i, err)
		}
		if err := json.NewEncoder(writer).Encode(value); err != nil {
			return xerrors.Errorf("encode item %d: %w", i, err)
		}
		i++
	}
	if reader.Err() != nil {
		return xerrors.Errorf("read: %w", err)
	}
	return nil
}

func CanonizeDynamicYtTable(t *testing.T, ytClient yt.Client, tablePath ypath.Path, fileName string) {
	file, err := os.Create(fileName)
	require.NoError(t, err)
	require.NoError(t, DumpDynamicYTTable(ytClient, tablePath, file))
	require.NoError(t, file.Close())
	canon.SaveFile(t, fileName, canon.WithLocal(true))
}

func YTTestDir(t *testing.T, testSuiteName string) ypath.Path {
	return ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt/%s/%s", testSuiteName, t.Name()))
}

func readAllRows[OutRow any](t *testing.T, ytEnv *yttest.Env, path ypath.Path) []OutRow {
	reader, err := ytEnv.YT.SelectRows(
		context.Background(),
		fmt.Sprintf("* from [%s]", path),
		nil,
	)
	require.NoError(t, err)

	outRows := make([]OutRow, 0)

	for reader.Next() {
		var row OutRow
		require.NoError(t, reader.Scan(&row), "Error reading row")
		outRows = append(outRows, row)
	}

	require.NoError(t, reader.Close())
	return outRows
}

func YTReadAllRowsFromAllTables[OutRow any](t *testing.T, cluster string, path string, expectedResCount int) []OutRow {
	ytEnv := yttest.New(t, yttest.WithConfig(yt.Config{Proxy: cluster}), yttest.WithLogger(logger.Log.Structured()))
	ytPath, err := ypath.Parse(path)
	require.NoError(t, err)

	exists, err := ytEnv.YT.NodeExists(context.Background(), ytPath.Path, nil)
	require.NoError(t, err)
	if !exists {
		return []OutRow{}
	}

	var tables []struct {
		Name string `yson:",value"`
	}

	require.NoError(t, ytEnv.YT.ListNode(context.Background(), ytPath, &tables, nil))

	resRows := make([]OutRow, 0, expectedResCount)
	for _, tableDesc := range tables {
		subPath := ytPath.Copy().Child(tableDesc.Name)
		readed := readAllRows[OutRow](t, ytEnv, subPath.Path)
		resRows = append(resRows, readed...)
	}
	return resRows
}
