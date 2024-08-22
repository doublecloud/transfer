package audittrails

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/audittrailsv1"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/elastic"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func makeMockElasticServer(t *testing.T, gotRequests chan<- string) *httptest.Server {
	return httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		buf := new(strings.Builder)
		_, err := io.Copy(buf, r.Body)
		require.NoError(t, err)

		logger.Log.Debugf("got:%s", buf.String())
		defer func() { gotRequests <- buf.String() }()

		urlPath := r.URL.Path
		if !strings.Contains(urlPath, "_bulk") {
			logger.Log.Debugf("unsupported mock endpoint:%s", buf.String())
			w.WriteHeader(http.StatusNotFound)
			return
		}

		respBody := `{"took":7, "errors": false, "items":[{"index":{"_index":"default-topic","_id":"1","_version":1,"result":"created","forced_refresh":false}}]}`
		w.Header().Add("Content-Type", "application/x-ndjson")
		w.Header().Add("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(respBody))
		require.NoError(t, err)
	}))

}

func TestAuditTrailsV1(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "yds source", Port: lbSendingPort},
	))

	//------------------------------------------------------------------------------

	parserConfigStruct := &audittrailsv1.ParserConfigAuditTrailsV1Common{
		UseElasticSchema: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &yds.YDSSource{
		Endpoint:       lbEnv.Endpoint,
		Port:           lbReceivingPort,
		Database:       "",
		Stream:         lbEnv.DefaultTopic,
		Consumer:       lbEnv.DefaultConsumer,
		Credentials:    lbEnv.Creds,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   parserConfigMap,
	}

	dst := &elastic.ElasticSearchDestination{}

	requestsToServer := make(chan string)

	mockServer := makeMockElasticServer(t, requestsToServer)
	mockServer.Start()
	defer mockServer.Close()
	esClient, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:            []string{mockServer.URL},
		UseResponseCheckOnly: true,
	})

	sink, err := elastic.NewSinkImpl(
		dst,
		logger.Log,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		esClient,
	)
	require.NoError(t, err)

	target := server.MockDestination{SinkerFactory: func() abstract.Sinker { return sink }}
	transfer := helpers.MakeTransfer("fake", src, &target, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer localWorker.Stop()

	//------------------------------------------------------------------------------
	// write data
	lines := []string{
		`{"event_id":"aje15tgjnqmbtcrqfeek","event_source":"iam","event_type":"yandex.cloud.audit.iam.DeleteServiceAccount","event_time":"2022-11-14T10:03:17Z","authentication":{"authenticated":true,"subject_type":"FEDERATED_USER_ACCOUNT","subject_id":"ajesnkfkc77lbh50isvg","subject_name":"mirtov8@yandex-team.ru"},"authorization":{"authorized":true},"resource_metadata":{"path":[{"resource_type":"organization-manager.organization","resource_id":"yc.organization-manager.yandex","resource_name":"yc-organization-manager-yandex"},{"resource_type":"resource-manager.cloud","resource_id":"b1g3o4minpkuh10pd2rj","resource_name":"arch"},{"resource_type":"resource-manager.folder","resource_id":"b1gvnphpkgt8oechmpo0","resource_name":"mirtov-webinar"}]},"request_metadata":{"remote_address":"2a02:6b8:b081:6421::1:3","user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15","request_id":"eeb31a5a-3a70-4c46-83c7-e8115c383f6e"},"event_status":"STARTED","details":{"service_account_id":"ajebgeotfmlu8os67phc","service_account_name":"yc-kyverno-image-verify"}}`,
	}
	helpers.WriteToLb(t, lbEnv.ProducerOptions().Endpoint, lbEnv.DefaultTopic, lbSendingPort, logbroker.LbDestination{Credentials: lbEnv.Creds, TLS: logbroker.DisabledTLS}, lines)

	//------------------------------------------------------------------------------
	// wait
	var canonData []string
	for len(canonData) < len(lines) {
		data := <-requestsToServer
		canonData = append(canonData, data)
	}
	close(requestsToServer)
	canon.SaveJSON(t, canonData)
}
