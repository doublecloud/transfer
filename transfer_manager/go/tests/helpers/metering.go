package helpers

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	dpconfig "github.com/doublecloud/tross/transfer_manager/go/pkg/config/dataplane"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer/logbroker"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
)

func MakeMeteringDPConfig(lbEnv *lbenv.LBEnv, topics []string) *dpconfig.CommonConfig {
	topicsMap := make(map[string]string)
	for _, topic := range topics {
		topicsMap[topic] = lbEnv.DefaultTopic
	}

	return &dpconfig.CommonConfig{
		Metering: &config.MeteringEnabled{
			Writers: []config.WriterEntry{
				{
					Writer: &logbroker.Logbroker{
						Endpoint: lbEnv.Endpoint,
						Creds:    nil,
						Database: "",
						TLSMode:  nil,
						Port:     lbEnv.Port,
					},
					MetricTopics: topicsMap,
				},
			},
		},
	}
}

func GetMeteringData(
	t *testing.T,
	lbEnv *lbenv.LBEnv,
	requiredRowCount int,
	ready func(meteringResult []MeteringMsg, requiredRowCount int) bool,
) ([]MeteringMsg, error) {
	meteringResult := make([]MeteringMsg, 0)
	// read metering messages (can be in two or four lb messages - 2 messages per send iteration - small and large metric)
	err := lbenv.CheckData(lbEnv.Env, "", lbEnv.DefaultTopic, func(msg string) bool {
		values, err := RemoveDynamicFields(msg)
		require.NoError(t, err)
		meteringResult = append(meteringResult, values...)
		logger.Log.Infof("Got %v metering messages", len(meteringResult))
		return ready(meteringResult, requiredRowCount)
	}, time.Second*60)
	return meteringResult, err
}

func RemoveDynamicFields(data string) ([]MeteringMsg, error) {
	var results []MeteringMsg

	jsonLines := strings.Split(data, "\n")
	for _, line := range jsonLines {
		var metering MeteringMsg
		err := json.Unmarshal([]byte(line), &metering)
		if err != nil {
			return nil, err
		}
		results = append(results, metering)
	}

	return results, nil
}

type MeteringMsg struct {
	CloudID    string `json:"cloud_id"`
	FolderID   string `json:"folder_id"`
	ResourceID string `json:"resource_id"`
	Schema     string `json:"schema"`
	Tags       Tags   `json:"tags"`
	Labels     Labels `json:"labels"`
	Usage      Usage  `json:"usage"`
	Version    string `json:"version"`
}

type Tags map[string]interface{}
type Labels map[string]interface{}

type DCTags struct {
	DstType      string `json:"dst_type"`
	RowSize      string `json:"row_size"`
	Runtime      string `json:"runtime"`
	SrcType      string `json:"src_type"`
	TransferType string `json:"transfer_type"`
}

type YCLabels struct {
	DstType      string `json:"dst_type"`
	SrcType      string `json:"src_type"`
	TransferType string `json:"transfer_type"`
}

type Usage struct {
	Quantity int    `json:"quantity"`
	Type     string `json:"type"`
	Unit     string `json:"unit"`
}
