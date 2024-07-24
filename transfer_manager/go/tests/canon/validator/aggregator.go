package validator

import (
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

type Aggregator struct {
	sinks []abstract.Sinker
}

func (c *Aggregator) Close() error {
	var errs util.Errors
	for _, sink := range c.sinks {
		if err := sink.Close(); err != nil {
			logger.Log.Error("unable to close", log.Error(err))
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return abstract.NewFatalError(xerrors.Errorf("sink failed to close: %w", errs))
}

func (c *Aggregator) Push(items []abstract.ChangeItem) error {
	var errs util.Errors
	for _, sink := range c.sinks {
		if err := sink.Push(items); err != nil {
			logger.Log.Error("unable to push", log.Error(err))
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return abstract.NewFatalError(xerrors.Errorf("sink failed to push: %w", errs))
}

func New(isStrictSource bool, factories ...func() abstract.Sinker) func() abstract.Sinker {
	return func() abstract.Sinker {
		var childSinks []abstract.Sinker
		for _, factory := range factories {
			childSinks = append(childSinks, factory())
		}
		if isStrictSource {
			childSinks = append(childSinks, valuesStrictTypeChecker())
		}
		return &Aggregator{
			sinks: childSinks,
		}
	}
}
