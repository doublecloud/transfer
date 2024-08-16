package tasks

import (
	"context"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/errors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/errors/categories"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/sink"
	"go.ytsaurus.tech/library/go/core/log"
)

func VerifyDelivery(transfer server.Transfer, lgr log.Logger, registry metrics.Registry) error {
	switch dst := transfer.Dst.(type) {
	case *postgres.PgDestination:
		// _ping and other tables created if MaintainTables is set to true
		dstMaintainTables := dst.MaintainTables
		dst.MaintainTables = true

		// restoring destination's MaintainTables value
		defer func() {
			dst.MaintainTables = dstMaintainTables
		}()
	}
	sink, err := sink.MakeAsyncSink(&transfer, lgr, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	if err := pingSinker(sink); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to ping sinker: %w", err)
	}

	factory, ok := providers.Source[providers.Verifier](lgr, registry, coordinator.NewFakeClient(), &transfer)
	if !ok {
		return nil
	}
	return factory.Verify(context.TODO())
}

func pingSinker(s abstract.AsyncSink) error {
	dropItem := []abstract.ChangeItem{
		{
			CommitTime:   uint64(time.Now().UnixNano()),
			Kind:         abstract.DropTableKind,
			Table:        "_ping",
			ColumnValues: []interface{}{"_ping"},
		},
	}

	err := <-s.AsyncPush(dropItem)
	if err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	err = <-s.AsyncPush([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Table:        "_ping",
			ColumnNames:  []string{"k", "_dummy"},
			ColumnValues: []interface{}{1, "nothing"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "k",
					DataType:   "int32",
					PrimaryKey: true,
				}, {
					ColumnName: "_dummy",
					DataType:   "string",
				},
			}),
		},
	})
	if err != nil {
		return xerrors.Errorf("unable to push: %w", err)
	}

	if err := <-s.AsyncPush(dropItem); err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	return nil
}
