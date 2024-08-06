package tasks

import (
	"context"
	"sync"
	"time"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const MaxTableStatCount = 1000

type SnapshotTableMetricsTracker struct {
	ctx             context.Context
	cancel          context.CancelFunc
	pushTicker      *time.Ticker
	waitForComplete sync.WaitGroup
	closed          bool

	transfer     *server.Transfer
	registry     metrics.Registry
	totalETA     float64
	tablesETAs   map[string]float64
	totalGauge   metrics.Gauge
	tablesGauges map[string]metrics.Gauge

	sharded bool

	// For non-sharded snapshot
	parts               []*server.OperationTablePart
	progressUpdateMutex *sync.Mutex

	// For sharded snapshot
	operationID string
	cpClient    coordinator.Coordinator
}

func NewNotShardedSnapshotTableMetricsTracker(
	ctx context.Context,
	transfer *server.Transfer,
	registry metrics.Registry,
	parts []*server.OperationTablePart,
	progressUpdateMutex *sync.Mutex,
) (*SnapshotTableMetricsTracker, error) {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableMetricsTracker{
		ctx:             ctx,
		cancel:          cancel,
		pushTicker:      nil,
		waitForComplete: sync.WaitGroup{},
		closed:          false,

		sharded: false,

		transfer:     transfer,
		registry:     registry,
		totalETA:     0,
		tablesETAs:   map[string]float64{},
		totalGauge:   nil,
		tablesGauges: map[string]metrics.Gauge{},

		parts:               parts,
		progressUpdateMutex: progressUpdateMutex,

		operationID: "",
		cpClient:    nil,
	}

	if err := tracker.init(); err != nil {
		return nil, xerrors.Errorf("Failed to init metrics tracker: %w", err)
	}

	tracker.waitForComplete.Add(1)
	tracker.pushTicker = time.NewTicker(time.Second * 15)
	go tracker.run()

	return tracker, nil
}

func NewShardedSnapshotTableMetricsTracker(
	ctx context.Context,
	transfer *server.Transfer,
	registry metrics.Registry,
	operationID string,
	cpClient coordinator.Coordinator,
) (*SnapshotTableMetricsTracker, error) {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableMetricsTracker{
		ctx:             ctx,
		cancel:          cancel,
		pushTicker:      nil,
		waitForComplete: sync.WaitGroup{},
		closed:          false,

		sharded: true,

		transfer:     transfer,
		registry:     registry,
		totalETA:     0,
		tablesETAs:   map[string]float64{},
		totalGauge:   nil,
		tablesGauges: map[string]metrics.Gauge{},

		parts:               nil,
		progressUpdateMutex: nil,

		operationID: operationID,
		cpClient:    cpClient,
	}

	if err := tracker.init(); err != nil {
		return nil, xerrors.Errorf("Failed to init metrics tracker: %w", err)
	}

	tracker.waitForComplete.Add(1)
	tracker.pushTicker = time.NewTicker(time.Second * 15)
	go tracker.run()

	return tracker, nil
}

func (t *SnapshotTableMetricsTracker) run() {
	defer t.waitForComplete.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.pushTicker.C:
			t.setMetrics()
		}
	}
}

// Close
// Safe to close few time, not thread safe;
// But safe to use with defer and standalone call in same time;
func (t *SnapshotTableMetricsTracker) Close() {
	if t.closed {
		return
	}

	t.pushTicker.Stop()
	t.cancel()
	t.waitForComplete.Wait()

	t.setMetrics()
	t.closed = true
}

func (t *SnapshotTableMetricsTracker) getTablesPartsNotSharded() []*server.OperationTablePart {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	partsCopy := make([]*server.OperationTablePart, 0, len(t.parts))
	for _, table := range t.parts {
		partsCopy = append(partsCopy, table.Copy())
	}
	return partsCopy
}

func (t *SnapshotTableMetricsTracker) getTablesParts() []*server.OperationTablePart {
	if t.sharded {
		parts, err := t.cpClient.GetOperationTablesParts(t.operationID)
		if err != nil {
			logger.Log.Error("Failed to get tables for update metrics", log.Error(err))
			return nil
		}
		return parts
	}

	return t.getTablesPartsNotSharded()
}

func (t *SnapshotTableMetricsTracker) init() error {
	parts := t.getTablesParts()
	for _, part := range parts {
		t.totalETA += float64(part.ETARows)

		tableKey := part.TableFQTN()
		if _, ok := t.tablesETAs[tableKey]; ok {
			t.tablesETAs[tableKey] += float64(part.ETARows)
		} else if len(t.tablesETAs) < MaxTableStatCount {
			t.tablesETAs[tableKey] = float64(part.ETARows)
		}
	}

	t.totalGauge = t.registry.Gauge("task.snapshot.reminder.total")
	t.totalGauge.Set(t.totalETA)

	for tableKey, tableETA := range t.tablesETAs {
		gauge := t.registry.WithTags(map[string]string{
			"table": tableKey,
		}).Gauge("task.snapshot.remainder.table")
		gauge.Set(tableETA)
		t.tablesGauges[tableKey] = gauge
	}

	return nil
}

func (t *SnapshotTableMetricsTracker) setMetrics() {
	parts := t.getTablesParts()
	if len(parts) <= 0 {
		return
	}

	totalCompleted := float64(0)
	tablesCompleted := map[string]float64{}
	for _, part := range parts {
		totalCompleted += float64(part.CompletedRows)

		tableKey := part.TableFQTN()
		if _, ok := t.tablesETAs[tableKey]; ok {
			tablesCompleted[tableKey] += float64(part.CompletedRows)
		}
	}

	t.totalGauge.Set(t.totalETA - totalCompleted)

	for tableKey, tableGauge := range t.tablesGauges {
		if eta, ok := t.tablesETAs[tableKey]; ok {
			if completed, ok := tablesCompleted[tableKey]; ok {
				tableGauge.Set(eta - completed)
			}
		}
	}
}
