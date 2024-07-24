package fake

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func TestFakeSession(t *testing.T) {
	const topic = "default-topic"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	logger := corelogadapter.NewTest(t)
	serv, err := NewFake(ctx, logger)
	require.NoError(t, err)
	require.NotNil(t, serv)

	address := serv.Address()
	chunks := strings.Split(address, ":")
	require.Len(t, chunks, 2)
	endpoint := chunks[0]
	port, err := strconv.Atoi(chunks[1])
	require.NoError(t, err)

	err = serv.CreateTopic(topic, 10)
	require.NoError(t, err)

	ss, err := session.Dial(ctx, session.Options{
		Endpoint: endpoint,
		Port:     port,
		Logger:   logger,
	}.WithProxy(address))

	require.NoError(t, err)

	defer func() { _ = ss.Close() }()
}

func TestFakeWriter(t *testing.T) {
	const topic = "default-topic"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	logger := corelogadapter.NewTest(t)
	serv, err := NewFake(ctx, logger)
	require.NoError(t, err)
	require.NotNil(t, serv)

	address := serv.Address()
	chunks := strings.Split(address, ":")
	require.Len(t, chunks, 2)
	endpoint := chunks[0]
	port, err := strconv.Atoi(chunks[1])
	require.NoError(t, err)

	err = serv.CreateTopic(topic, 10)
	require.NoError(t, err)

	options := persqueue.WriterOptions{
		Endpoint:       endpoint,
		Port:           port,
		Logger:         logger,
		Topic:          topic,
		SourceID:       []byte(fmt.Sprintf("gotest/%s", uuid.Must(uuid.NewV4()))),
		Codec:          persqueue.Raw,
		RetryOnFailure: true,
	}.WithProxy(address)

	p := persqueue.NewWriter(options)

	_, err = p.Init(ctx)
	require.NoError(t, err)

	defer func() { _ = p.Close() }()

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("foo")}))
	require.NoError(t, p.Close())
}

func TestFakeReader(t *testing.T) {
	const topic = "default-topic"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	logger := corelogadapter.NewTest(t)
	serv, err := NewFake(ctx, logger)
	require.NoError(t, err)
	require.NotNil(t, serv)

	address := serv.Address()
	chunks := strings.Split(address, ":")
	require.Len(t, chunks, 2)
	endpoint := chunks[0]
	port, err := strconv.Atoi(chunks[1])
	require.NoError(t, err)

	err = serv.CreateTopic(topic, 10)
	require.NoError(t, err)

	options := persqueue.WriterOptions{
		Endpoint:       endpoint,
		Port:           port,
		Logger:         logger,
		Topic:          topic,
		SourceID:       []byte(fmt.Sprintf("gotest/%s", uuid.Must(uuid.NewV4()))),
		Codec:          persqueue.Raw,
		RetryOnFailure: true,
	}.WithProxy(address)

	p := persqueue.NewWriter(options)
	_, err = p.Init(ctx)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, p.Close())
	}()

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("privet")}))
	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("_")}))

	consOptions := persqueue.ReaderOptions{
		Endpoint:              endpoint,
		Port:                  port,
		Consumer:              "test_client",
		Topics:                []persqueue.TopicInfo{{Topic: options.Topic}},
		MaxReadSize:           100 * 1024 * 1024, // 100 mb
		DecompressionDisabled: false,
		RetryOnFailure:        true,
		Logger:                logger,
	}.WithProxy(address)

	c := persqueue.NewReader(consOptions)

	_, err = c.Start(ctx)
	require.NoError(t, err)

	expectedData := map[string]struct{}{"_": struct{}{}, "privet": struct{}{}}
	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.Data:
			for _, batch := range m.Batches() {
				for _, msg := range batch.Messages {
					data := string(msg.Data)
					require.Contains(t, expectedData, data)
					delete(expectedData, data)
				}
			}
			m.Commit()
			c.Shutdown()

		case *persqueue.CommitAck:
		default:
			t.Fatalf("Received unexpected event %T", m)
		}
	}
	require.Empty(t, expectedData)
	require.NoError(t, c.Err())
}

func TestFakeReaderLock(t *testing.T) {
	const topic = "default-topic"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	logger := corelogadapter.NewTest(t)
	serv, err := NewFake(ctx, logger)
	require.NoError(t, err)
	require.NotNil(t, serv)

	address := serv.Address()
	chunks := strings.Split(address, ":")
	require.Len(t, chunks, 2)
	endpoint := chunks[0]
	port, err := strconv.Atoi(chunks[1])
	require.NoError(t, err)

	err = serv.CreateTopic(topic, 10)
	require.NoError(t, err)

	options := persqueue.WriterOptions{
		Endpoint:       endpoint,
		Port:           port,
		Logger:         logger,
		Topic:          topic,
		SourceID:       []byte(fmt.Sprintf("gotest/%s", uuid.Must(uuid.NewV4()))),
		Codec:          persqueue.Raw,
		RetryOnFailure: false,
	}.WithProxy(address)

	p := persqueue.NewWriter(options)
	_, err = p.Init(ctx)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, p.Close())
	}()

	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("_")}))

	consOptions := persqueue.ReaderOptions{
		Endpoint:                  endpoint,
		Port:                      port,
		Consumer:                  "test_client",
		Topics:                    []persqueue.TopicInfo{{Topic: options.Topic}},
		MaxReadSize:               100 * 1024 * 1024, // 100 mb
		DecompressionDisabled:     false,
		RetryOnFailure:            false,
		ManualPartitionAssignment: true,
		Logger:                    logger,
	}.WithProxy(address)

	c := persqueue.NewReader(consOptions)

	_, err = c.Start(ctx)
	require.NoError(t, err)

	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.Data: // read needed to ensure we get response after start read
			m.Commit()
			c.Shutdown()
		case *persqueue.CommitAck:

		case *persqueue.Lock: // actual test here
			m.StartRead(true, m.ReadOffset, m.ReadOffset)
		default:
			t.Fatalf("Received unexpected event %T", m)
		}
	}
	require.NoError(t, c.Err())
}

func TestFakeWriteAfterDiscoinnect(t *testing.T) {
	const topic = "default-topic"
	ctx, stop := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer stop()
	lbCtx, lbStop := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	defer lbStop()

	logger := corelogadapter.NewTest(t)
	serv, err := NewFake(lbCtx, logger)
	require.NoError(t, err)
	require.NotNil(t, serv)

	address := serv.Address()
	chunks := strings.Split(address, ":")
	require.Len(t, chunks, 2)
	endpoint := chunks[0]
	port, err := strconv.Atoi(chunks[1])
	require.NoError(t, err)

	err = serv.CreateTopic(topic, 10)
	require.NoError(t, err)

	options := persqueue.WriterOptions{
		Endpoint:       endpoint,
		Port:           port,
		Logger:         logger,
		Topic:          topic,
		SourceID:       []byte(fmt.Sprintf("gotest/%s", uuid.Must(uuid.NewV4()))),
		Codec:          persqueue.Raw,
		RetryOnFailure: false,
	}.WithProxy(address)

	p := persqueue.NewWriter(options)

	_, err = p.Init(ctx)
	require.NoError(t, err)
	require.NoError(t, p.Write(&persqueue.WriteMessage{Data: []byte("foo")}))

	waitClose := time.NewTimer(time.Second * 2)
	select {
	case <-waitClose.C:
		require.False(t, true, "No error after fake death!")
	case <-p.Closed(): // Expect closed.
	}
}
