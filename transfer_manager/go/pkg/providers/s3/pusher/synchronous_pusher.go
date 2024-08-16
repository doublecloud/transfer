package pusher

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
)

type SyncPusher struct {
	pusher abstract.Pusher
}

func (p *SyncPusher) Push(_ context.Context, chunk Chunk) error {
	if err := p.pusher(chunk.Items); err != nil {
		return xerrors.Errorf("failed to push: %w", err)
	}
	return nil
}

func (p *SyncPusher) Ack(chunk Chunk) (bool, error) {
	// should not be used anyway
	return false, nil
}

func NewSyncPusher(pusher abstract.Pusher) *SyncPusher {
	return &SyncPusher{
		pusher: pusher,
	}
}
