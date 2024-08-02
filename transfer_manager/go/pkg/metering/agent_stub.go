package metering

import (
	"context"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
)

type stubAgent struct {
	logger log.Logger
}

func (*stubAgent) SetOpts(*MeteringOpts) error {
	return nil
}

func (*stubAgent) CountInputRows(items []abstract.ChangeItem)  {}
func (*stubAgent) CountOutputRows(items []abstract.ChangeItem) {}
func (*stubAgent) CountOutputBatch(input base.EventBatch)      {}

func (sa *stubAgent) RunPusher(ctx context.Context, interval time.Duration) error {
	sa.logger.Warn("it is stub metering agent, exiting...")
	return nil
}

func (sa *stubAgent) Stop() error {
	return nil
}

func NewStubAgent(lgr log.Logger) MeteringAgent {
	return &stubAgent{
		logger: lgr,
	}
}
