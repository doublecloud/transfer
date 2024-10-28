package tasks

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/util"
)

func AddExtraTransformers(ctx context.Context, transfer *model.Transfer, registry metrics.Registry) error {
	if transformableSource, ok := transfer.Src.(model.ExtraTransformableSource); ok {
		transformers, err := transformableSource.ExtraTransformers(ctx, transfer, registry)
		if err != nil {
			return xerrors.Errorf("cannot set extra transformers from source: %w", err)
		}

		err = util.ForEachErr(transformers, func(transformer abstract.Transformer) error {
			return transfer.AddExtraTransformer(transformer)
		})
		if err != nil {
			return xerrors.Errorf("an error occured during adding a transformation: %w", err)
		}
	}
	return nil
}
