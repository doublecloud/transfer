package clickhouse

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	sink_factory "github.com/doublecloud/transfer/pkg/sink"
)

func (p *Provider) loadClickHouseSchema() error {
	if _, ok := p.transfer.Src.(*model.ChSource); !ok {
		return nil
	}
	if _, ok := p.transfer.Dst.(*model.ChDestination); !ok {
		return nil
	}
	sink, err := sink_factory.MakeAsyncSink(p.transfer, p.logger, p.registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return xerrors.Errorf("unable to make sinker: %w", err)
	}
	defer sink.Close()
	storage, err := p.Storage()
	if err != nil {
		return xerrors.Errorf("failed to resolve storage: %w", err)
	}
	defer storage.Close()
	tables, err := server.FilteredTableList(storage, p.transfer)
	if err != nil {
		return xerrors.Errorf("failed to list tables and their schemas: %w", err)
	}
	chStorage := storage.(*Storage)
	if err := chStorage.CopySchema(tables, abstract.PusherFromAsyncSink(sink)); err != nil {
		return xerrors.Errorf("unable to copy clickhouse schema: %w", err)
	}
	return nil
}
