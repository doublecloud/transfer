package lbenv

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/stretchr/testify/require"
)

type Comparator func(got string, index int) bool

func ComparatorStub(in string, index int) bool {
	return true
}

func EqualLogLineContent(t *testing.T, expected, real string) bool {
	var j map[string]interface{}
	err := json.Unmarshal([]byte(real), &j)
	require.NoError(t, err)
	delete(j, "ts")
	delete(j, "caller")
	jj, err := json.Marshal(j)
	require.NoError(t, err)

	return string(jj) == expected
}

func CheckResult(t *testing.T, lbEnv *recipe.Env, database, topic string, expectedNum int, dataCmp, extraCmp, sourceIDCmp, IPCmp, batchTopicCmp Comparator) {
	readerOptions := lbEnv.ConsumerOptions()
	readerOptions.Database = database
	readerOptions.Topics = []persqueue.TopicInfo{{Topic: topic}}
	readerOptions.WithProxy(fmt.Sprintf("%s:%d", lbEnv.Endpoint, lbEnv.Port))

	c := persqueue.NewReader(readerOptions)

	newContext, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFunc()

	_, err := c.Start(newContext)
	require.NoError(t, err)

	index := 0
	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.Data:
			for _, batch := range m.Batches() {
				require.True(t, batchTopicCmp(batch.Topic, 0), fmt.Sprintf("failing with batch.Topic '%s'", batch.Topic))
				for _, msg := range batch.Messages {
					data := string(msg.Data)
					require.True(t, dataCmp(data, index))

					require.True(t, IPCmp(msg.IP, index), fmt.Sprintf("failing with msg.IP '%s'", msg.IP))

					sourceID := string(msg.SourceID)
					require.True(t, sourceIDCmp(sourceID, index), fmt.Sprintf("failing with sourceID '%s'", sourceID))

					extraFieldsBuf, err := json.Marshal(msg.ExtraFields)
					require.NoError(t, err)
					require.True(t, extraCmp(string(extraFieldsBuf), index), fmt.Sprintf("failing with extraFieldsBuf '%s'", string(extraFieldsBuf)))

					index++
				}
			}
			m.Commit()
			if index >= expectedNum {
				c.Shutdown()
			}

		case *persqueue.CommitAck:
		default:
			t.Fatalf("Received unexpected event %T", m)
		}
	}

	err = c.Err()
	if err == context.DeadlineExceeded {
		require.Equal(t, expectedNum, index)
		return
	}
	require.Equal(t, expectedNum, index)
	require.NoError(t, c.Err())
}

func CheckData(lbEnv *recipe.Env, database, topic string, checkFunc func(msg string) bool, maxDuration time.Duration) error {
	sleepTime := time.Second * 2
	startTime := time.Now()
	readerOptions := lbEnv.ConsumerOptions()
	readerOptions.Database = database
	readerOptions.Topics = []persqueue.TopicInfo{{Topic: topic}}
	readerOptions.WithProxy(fmt.Sprintf("%s:%d", lbEnv.Endpoint, lbEnv.Port))

	c := persqueue.NewReader(readerOptions)

	newContext, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFunc()

	_, err := c.Start(newContext)
	if err != nil {
		return err
	}
	checked := false
	for e := range c.C() {
		duration := time.Since(startTime)
		if duration > maxDuration {
			return xerrors.Errorf("Exceeded max allowed duration checks")
		}
		switch m := e.(type) {
		case *persqueue.Data:
			for _, batch := range m.Batches() {
				for _, msg := range batch.Messages {
					data := string(msg.Data)
					checked = checked || checkFunc(data)
				}
			}
			m.Commit()
			if checked {
				c.Shutdown()
			}
		case *persqueue.CommitAck:
		default:
			return xerrors.Errorf("Received unexpected event %T", m)
		}
		logger.Log.Warnf("Check func returned false, will sleep %v and retry", sleepTime)
		time.Sleep(sleepTime)
	}

	err = c.Err()
	if err != nil && err != context.DeadlineExceeded {
		return xerrors.Errorf("reader error: %w", err)
	}
	return nil
}
