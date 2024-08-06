package airbyte

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Source = (*Source)(nil)

type Source struct {
	registry metrics.Registry
	cp       coordinator.Coordinator
	logger   log.Logger
	config   *AirbyteSource
	catalog  *Catalog
	metrics  *stats.SourceStats
	transfer *server.Transfer

	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	defer s.wg.Done()
	s.wg.Add(1)
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.InitialInterval = time.Second * 10
	backoffTimer.MaxElapsedTime = 0
	backoffTimer.Reset()
	nextWaitDuration := backoffTimer.NextBackOff()
	storage, _ := NewStorage(s.logger, s.registry, s.cp, s.config, s.transfer)
	tables, err := storage.TableList(s.transfer)
	if err != nil {
		return xerrors.Errorf("unable to list object: %w", err)
	}
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		default:
		}

		cntr := 0
		for tid := range tables {
			if err := storage.LoadTable(s.ctx, abstract.TableDescription{
				Name:   tid.Name,
				Schema: tid.Namespace,
				Filter: "",
				EtaRow: 0,
				Offset: 0,
			}, func(items []abstract.ChangeItem) error {
				cntr += len(items)
				return <-sink.AsyncPush(items)
			}); err != nil {
				return xerrors.Errorf("unable to load table: %w", err)
			}
		}
		if cntr == 0 {
			s.logger.Infof("No new rows from airbyte found, will wait %v", nextWaitDuration)
			time.Sleep(nextWaitDuration)
			nextWaitDuration = backoffTimer.NextBackOff()
			continue
		}
		backoffTimer.Reset()
		s.logger.Infof("Done %v rows, will wait %v", cntr, nextWaitDuration)
	}
}

func (s *Source) Stop() {
	s.cancel()
	s.wg.Wait()
}

func NewSource(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, cfg *AirbyteSource, transfer *server.Transfer) *Source {
	ctx, cancel := context.WithCancel(context.Background())
	return &Source{
		registry: registry,
		cp:       cp,
		logger:   lgr,
		config:   cfg,
		catalog:  nil,
		metrics:  stats.NewSourceStats(registry),
		transfer: transfer,
		ctx:      ctx,
		cancel:   cancel,
		wg:       sync.WaitGroup{},
	}
}
