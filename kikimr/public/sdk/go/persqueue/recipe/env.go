// Package recipe is helper for running integration tests in arcadia.
package recipe

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/credentials"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
	ydbCredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

type Env struct {
	Endpoint string
	Port     int
	DC       string
	Creds    credentials.Credentials

	// DefaultTopic is name of topic, that might be used for testing.
	//
	// "default-topic" when running under ya make -t.
	//
	// /logbroker-playground/${USER}/demo-topic when running against production cluster.
	DefaultTopic string

	DefaultConsumer string

	logger log.Logger
}

func (e *Env) ProducerOptions() persqueue.WriterOptions {
	return persqueue.WriterOptions{
		Endpoint:       e.Endpoint,
		Port:           e.Port,
		Credentials:    e.Creds,
		Logger:         e.logger,
		Topic:          e.DefaultTopic,
		SourceID:       []byte(fmt.Sprintf("gotest/%s", uuid.Must(uuid.NewV4()))),
		Codec:          persqueue.Gzip,
		RetryOnFailure: true,
	}
}

func (e *Env) ConsumerOptions() persqueue.ReaderOptions {
	return persqueue.ReaderOptions{
		Endpoint:              e.Endpoint,
		Port:                  e.Port,
		Credentials:           e.Creds,
		Consumer:              e.DefaultConsumer,
		Topics:                []persqueue.TopicInfo{{Topic: e.DefaultTopic}},
		MaxReadSize:           100 * 1024 * 1024, // 100 mb
		DecompressionDisabled: false,
		RetryOnFailure:        true,
		Logger:                e.logger,
	}
}

// New returns parameters required for connection to LB cluster.
//
// New has two modes of operation.
//
// When invoked from inside ya make -t, returns parameters for local LB instance.
//
// When LOGBROKER_ENDPOINT environment variable is set, returns parameters for production LB cluster. Useful for local testing. Use this option with care.
func New(t *testing.T) *Env {
	endpoint, err := ioutil.ReadFile("ydb_endpoint.txt")
	if os.Getenv("LOGBROKER_PORT") != "" {
		endpoint = []byte(os.Getenv("LOGBROKER_PORT"))
	}

	testLogger := corelogadapter.NewTest(t)
	if err != nil && os.IsNotExist(err) {
		endpoint := os.Getenv("LOGBROKER_ENDPOINT")
		if endpoint == "" {
			t.Fatalf("Can't locate LB cluster")
		}

		u, err := user.Current()
		require.NoError(t, err)

		home, err := os.UserHomeDir()
		require.NoError(t, err)

		token, err := ioutil.ReadFile(filepath.Join(home, ".logbroker", "token"))
		require.NoError(t, err)

		tokenStr := strings.TrimSuffix(string(token), "\n")

		return &Env{
			Endpoint:        endpoint,
			DC:              "vla",
			Creds:           ydbCredentials.NewAccessTokenCredentials(tokenStr),
			DefaultTopic:    fmt.Sprintf("/logbroker-playground/%s/demo-topic", u.Username),
			DefaultConsumer: fmt.Sprintf("/logbroker-playground/%s/demo-consumer", u.Username),

			logger: testLogger,
		}
	}

	require.NoError(t, err)
	env := &Env{
		Endpoint:        "localhost",
		DC:              "dc1",
		DefaultTopic:    "default-topic",
		DefaultConsumer: "test_client",
		logger:          testLogger,
	}

	tokens := strings.Split(string(endpoint), ":")
	env.Port, err = strconv.Atoi(strings.Trim(tokens[len(tokens)-1], " \n"))
	require.NoError(t, err)

	return env
}
