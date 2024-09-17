package problemitemdetector

import (
	"encoding/json"
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

func PluggableProblemItemTransformer(transfer *server.Transfer, _ metrics.Registry, _ coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker {
	if transfer.Transformation == nil || transfer.Transformation.Transformers == nil {
		return IdentityMiddleware
	}

	lgr := transferNeedDetector(transfer.Transformation.Transformers)
	if lgr == nil {
		return IdentityMiddleware
	}

	return func(s abstract.Sinker) abstract.Sinker {
		return newPluggableTransformer(s, lgr)
	}
}

var IdentityMiddleware = func(s abstract.Sinker) abstract.Sinker { return s }

func transferNeedDetector(transformers *transformer.Transformers) log.Logger {
	for _, t := range transformers.Transformers {
		if v, ok := t[TransformerType]; ok {
			if detector, isProblemItemDetector := v.(*problemItemDetector); isProblemItemDetector {
				return detector.logger
			}
		}
	}

	return nil
}

type pluggableTransformer struct {
	sink   abstract.Sinker
	logger log.Logger
}

func newPluggableTransformer(s abstract.Sinker, logger log.Logger) abstract.Sinker {
	return &pluggableTransformer{s, logger}
}

func (d *pluggableTransformer) Close() error {
	return d.sink.Close()
}

func (d *pluggableTransformer) Push(items []abstract.ChangeItem) error {
	err := d.sink.Push(items)
	if err != nil {
		return d.pushProblemItemsSlice(items, err)
	}
	return nil
}

func (d *pluggableTransformer) pushProblemItemsSlice(items []abstract.ChangeItem, logError error) error {
	for i := range items {
		if err := d.sink.Push([]abstract.ChangeItem{items[i]}); err != nil {
			return d.logProblemItem(items[i], logError)
		}
	}

	return nil
}

func (d *pluggableTransformer) logProblemItem(item abstract.ChangeItem, logError error) error {
	d.logger.Error(fmt.Sprintf("problem_item_detector - found problem item, table '%s'", item.Fqtn()), log.Error(logError))

	// to avoid problem with len of log convert the values and log them separately
	for i, value := range item.ColumnValues {
		if encodedVal, err := json.Marshal(item.ColumnValues[i]); err == nil {
			d.logger.Errorf("problem_item_detector - type: %T, column %s : %s", value, item.ColumnNames[i], string(encodedVal))
		} else {
			d.logger.Errorf("problem_item_detector (unable marshal value) - type: %T, column %s : %v", value, item.ColumnNames[i], value)
		}
	}
	for i, value := range item.OldKeys.KeyValues {
		if encodedVal, err := json.Marshal(item.OldKeys.KeyValues[i]); err == nil {
			d.logger.Errorf("problem_item_detector - OldKeys type: %T, column %s : %s", value, item.OldKeys.KeyNames[i], string(encodedVal))
		} else {
			d.logger.Errorf("problem_item_detector (unable marshal value) - OldKeys type: %T, column %s : %v", value, item.OldKeys.KeyNames[i], value)
		}
	}

	return abstract.NewFatalError(xerrors.New("bad item detector found problem item"))
}

func init() {
	middlewares.PlugTransformer(PluggableProblemItemTransformer)
}
