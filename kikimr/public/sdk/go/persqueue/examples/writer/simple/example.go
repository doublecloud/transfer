package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

// [BEGIN init options]
var (
	endpoint = flag.String("endpoint", "vla.logbroker.yandex.net", "Logbroker endpoint")
	token    = flag.String("token", os.Getenv("LOGBROKER_TOKEN"), "LB OAuth token, default LOGBROKER_TOKEN env variable")
	topic    = flag.String("topic", "", "Topic")
	sourceID = flag.String("sourceID", "", "Message group")
	messages = flag.Int("messages", 1000, "Number of messages")
	database = flag.String("database", "/Root", "Database")
	useTLS   = flag.Bool("use-tls", false, "Use TLS connection")
)

var Log = corelogadapter.DefaultLogger

// [END init options]

func main() {
	flag.Parse()

	Log.Log(context.Background(), log.LevelInfo, "Starting example Logbroker writer: topic=%v, sourceID=%v", map[string]interface{}{
		"topic":    topic,
		"sourceID": sourceID,
	})

	// [BEGIN create new writer]
	// Step 1: Create new writer
	ctx := context.Background()
	var tlsConfig *tls.Config
	if *useTLS {
		tlsConfig = &tls.Config{}
	}
	p := persqueue.NewWriter(
		persqueue.WriterOptions{
			Endpoint:       *endpoint,
			Credentials:    credentials.NewAccessTokenCredentials(*token),
			TLSConfig:      tlsConfig,
			Database:       *database,
			Logger:         Log,
			Topic:          *topic,
			SourceID:       []byte(*sourceID),
			Codec:          persqueue.Gzip,
			RetryOnFailure: true,
		},
	)

	// Step 2: Init writer. Here we ignore WriterInit.
	init, err := p.Init(ctx)
	if err != nil {
		Log.Log(ctx, log.LevelFatal, "Unable to start writer", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Step 3: Init goroutine receiving feedback.
	go func(p persqueue.Writer) (issues []*persqueue.Issue) {
		for rsp := range p.C() {
			switch m := rsp.(type) {
			case *persqueue.Ack:
				Log.Log(ctx, log.LevelDebug, "Ack", map[string]interface{}{
					"seqNo": m.SeqNo,
				})
			case *persqueue.Issue:
				Log.Log(ctx, log.LevelError, "Got error", map[string]interface{}{
					"error": m.Err.Error(),
				})
				issues = append(issues, m)
			}
		}
		return
	}(p)
	// [END create new writer]

	// [BEGIN write messages]
	// Generate next sequence number
	seqNo := init.MaxSeqNo + 1

	// Step 3: Write messages.
	for i := 0; i < *messages; i++ {
		data := &persqueue.WriteMessage{
			Data: []byte(fmt.Sprintf("Message seqNo: %v idx: %v", seqNo, i)),
		}
		data.WithSeqNo(seqNo)
		if err := p.Write(data); err != nil {
			Log.Log(ctx, log.LevelFatal, "Writer terminated", map[string]interface{}{
				"error": err.Error(),
			})
		}
		seqNo++
	}

	// Step 4. Wait until all messages are delivered to Logbroker and terminate.
	if err := p.Close(); err != nil {
		Log.Log(ctx, log.LevelFatal, "Writer terminated", map[string]interface{}{
			"error": err.Error(),
		})
	}
	// [END write messages]
}
