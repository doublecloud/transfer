package eventsource

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/targets/legacy"
)

type eventSourceSource struct {
	source        base.EventSource
	cleanupPolicy server.CleanupType
	tmpPolicy     *server.TmpPolicyConfig

	logger log.Logger
}

// NewSource constructs a wrapper over the given base.EventSource with the abstract.Source interface
func NewSource(logger log.Logger, source base.EventSource, cleanupPolicy server.CleanupType, tmpPolicy *server.TmpPolicyConfig) abstract.Source {
	return &eventSourceSource{
		source:        source,
		cleanupPolicy: cleanupPolicy,
		tmpPolicy:     tmpPolicy,

		logger: logger,
	}
}

func (s *eventSourceSource) Run(sink abstract.AsyncSink) error {
	if s.source.Running() {
		return xerrors.New("Source is already in running state")
	}

	target := legacy.NewEventTarget(s.logger, sink, s.cleanupPolicy, s.tmpPolicy)
	if err := s.source.Start(context.Background(), target); err != nil {
		return err
	}
	return nil
}

func (s *eventSourceSource) Stop() {
	if err := s.source.Stop(); err != nil {
		s.logger.Error("Error on source stop", log.Error(err))
	}
}
