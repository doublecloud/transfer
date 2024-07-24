package tasks

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
)

func AddExtraTransformers(ctx context.Context, transfer *server.Transfer, registry metrics.Registry) error {
	if transformableSource, ok := transfer.Src.(server.ExtraTransformableSource); ok {
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
