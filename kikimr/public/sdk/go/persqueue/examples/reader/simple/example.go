package main

import (
	"context"
	"crypto/tls"
	"flag"
	_ "net/http/pprof"
	"os"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

// [BEGIN init options]
var (
	endpoint = flag.String("endpoint", "vla.logbroker.yandex.net", "")
	token    = flag.String("token", os.Getenv("LOGBROKER_TOKEN"), "LB OAuth token, default LOGBROKER_TOKEN env variable")
	topic    = flag.String("topic", "Topic", "")
	consumer = flag.String("consumer", "Consumer", "")
	database = flag.String("database", "/Root", "Database")
	useTLS   = flag.Bool("use-tls", false, "Use TLS connection")
)

var (
	Log = corelogadapter.DefaultLogger
)

// [END init options]

func main() {
	flag.Parse()

	// [BEGIN create reader]
	var tlsConfig *tls.Config
	if *useTLS {
		tlsConfig = &tls.Config{}
	}
	c := persqueue.NewReader(persqueue.ReaderOptions{
		Endpoint:              *endpoint,
		Credentials:           credentials.NewAccessTokenCredentials(*token),
		TLSConfig:             tlsConfig,
		Database:              *database,
		Consumer:              *consumer,
		Logger:                Log,
		Topics:                []persqueue.TopicInfo{{Topic: *topic}},
		MaxReadSize:           8 * 1024, // 100 kb
		MaxReadMessagesCount:  1,
		DecompressionDisabled: true,
		RetryOnFailure:        true,
	})

	ctx := context.Background()
	if _, err := c.Start(ctx); err != nil {
		Log.Log(ctx, log.LevelFatal, "Unable to start reader", map[string]interface{}{
			"error": err.Error(),
		})
	}
	// [END create reader]

	// [BEGIN read]
	for msg := range c.C() {
		switch v := msg.(type) {
		case *persqueue.Data:
			for _, b := range v.Batches() {
				Log.Log(ctx, log.LevelInfo, "Received batch", map[string]interface{}{
					"topic":       b.Topic,
					"partition":   b.Partition,
					"messagesLen": len(b.Messages),
				})

				for _, m := range b.Messages {
					Log.Log(ctx, log.LevelTrace, "Received message", map[string]interface{}{
						"seqNo": m.SeqNo,
					})
				}
			}
			v.Commit()

		default:
		}
	}

	<-c.Closed()
	if err := c.Err(); err != nil {
		Log.Log(ctx, log.LevelFatal, "Reader error", map[string]interface{}{
			"error": err.Error(),
		})
	}
	// [END read]
}
