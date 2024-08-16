package tasks

import (
	"errors"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/cleanup"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares/async/bufferer"
	"github.com/stretchr/testify/require"
)

type fakeSink struct {
	push func(items []abstract.ChangeItem) error
}

func newFakeSink(push func(items []abstract.ChangeItem) error) *fakeSink {
	return &fakeSink{push: push}
}

func (s *fakeSink) Close() error {
	return nil
}

func (s *fakeSink) Push(items []abstract.ChangeItem) error {
	return s.push(items)
}

func TestAsynchronousSnapshotStateNonRowItem(t *testing.T) {
	sink := newFakeSink(func(items []abstract.ChangeItem) error {
		return errors.New("some error")
	})

	bufferer := bufferer.Bufferer(logger.Log, bufferer.BuffererConfig{TriggingCount: 0, TriggingSize: 0, TriggingInterval: 0}, solomon.NewRegistry(nil))
	asyncSink := bufferer(sink)
	defer cleanup.Close(asyncSink, logger.Log)

	state := newAsynchronousSnapshotState(asyncSink)
	pusher := state.SnapshotPusher()
	require.Error(t, pusher([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad},
	}))
	require.NoError(t, state.Close())
}
