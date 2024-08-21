package testcase

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
)

type TestCase struct {
	TopicName    string
	ParserConfig parsers.AbstractParserConfig
	Data         persqueue.ReadMessage
}

func LoadStaticTestCases(t *testing.T, samples embed.FS) map[string]TestCase {
	cases := map[string]TestCase{}
	require.NoError(t, fs.WalkDir(samples, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".config.json") {
			return nil
		}
		caseName := strings.ReplaceAll(filepath.Base(path), ".config.json", "")
		// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
		fmt.Printf("timmyb32rQQQ:caseName=%s\n", caseName)
		// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
		configData, err := fs.ReadFile(samples, path)
		if err != nil {
			return err
		}
		sampleData, err := fs.ReadFile(samples, filepath.Dir(path)+"/"+caseName+".sample")
		if err != nil {
			return err
		}
		var source logbroker.LfSource
		if err := json.Unmarshal(configData, &source); err != nil {
			logger.Log.Warn("unable to unmarshal", log.Error(err))
			return nil
		}
		source.WithDefaults()
		parserConfig, err := parsers.ParserConfigMapToStruct(source.ParserConfig)
		require.NoError(t, err)

		cases[caseName] = TestCase{
			TopicName:    source.Topics[0],
			ParserConfig: parserConfig,
			Data:         MakeDefaultPersqueueReadMessage(sampleData),
		}
		return nil
	}))
	return cases
}

func MakeDefaultPersqueueReadMessage(data []byte) persqueue.ReadMessage {
	return persqueue.ReadMessage{
		Offset:      123,
		SeqNo:       32,
		SourceID:    []byte("test_source_id"),
		CreateTime:  time.Date(2020, 2, 2, 10, 2, 21, 0, time.UTC),
		WriteTime:   time.Date(2020, 2, 2, 10, 2, 20, 0, time.UTC),
		IP:          "192.168.1.1",
		Data:        data,
		ExtraFields: map[string]string{"some_field": "test"},
	}
}
