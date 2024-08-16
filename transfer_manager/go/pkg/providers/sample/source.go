package sample

import (
	"context"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	userLoginsSampleType   = "user_logins"
	devopsSampleType       = "devops"
	maxNumberOfUsageUsers  = 100
	maxNumberOfUsageSystem = 100
	maxNumberOfHosts       = 3
	maxNumberIot           = 100
	maxNumberOfDevices     = 3
)

var (
	_ abstract.Source = (*Source)(nil)
)

type Source struct {
	src        *SampleSource
	ctx        context.Context
	cancel     func()
	metrics    *stats.SourceStats
	transferID string
	logger     log.Logger
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	sleepTimer := backoff.NewExponentialBackOff()
	sleepTimer.InitialInterval = s.src.MinSleepTime
	sleepTimer.MaxElapsedTime = 0
	sleepTimer.Reset()

	defer s.cancel()
	errCh := make(chan error)

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		case err := <-errCh:
			if err != nil {
				return xerrors.Errorf("received async push error from generated data: %w", err)
			}
		default:
		}

		nextSleepDuration := sleepTimer.NextBackOff()
		if nextSleepDuration > 2*s.src.MinSleepTime {
			sleepTimer.Reset()
			nextSleepDuration = s.src.MinSleepTime
		}
		s.logger.Infof("sleep duration %v", nextSleepDuration)
		time.Sleep(nextSleepDuration)

		data := generateRandomDataForSampleType(s.src.SampleType, s.src.MaxSampleData, s.src.TableName, s.metrics)
		s.metrics.ChangeItems.Add(int64(len(data)))

		pushStart := time.Now()
		go func(errChAsync chan error, pushStart time.Time) {
			err := <-errChAsync
			if err != nil {
				ok := util.Send(s.ctx, errCh, err)
				if !ok {
					s.logger.Error("context was canceled")
					return
				}
				s.logger.Info("successfully pushed error into ErrCh channel")
			} else {
				s.metrics.PushTime.RecordDuration(time.Since(pushStart))
			}
		}(sink.AsyncPush(data), pushStart)

	}
}
func generateRandomDataForSampleType(sampleType string, maxRowSampleData int, table string, metrics *stats.SourceStats) []abstract.ChangeItem {
	generatedData := make([]abstract.ChangeItem, maxRowSampleData)
	var streamData StreamingData

	for i := 0; i < maxRowSampleData; i++ {
		switch sampleType {
		case userLoginsSampleType:
			user := gofakeit.Name()
			city := gofakeit.City()
			streamData = NewUserLogins(table, user, city, time.Now())
		case devopsSampleType:
			hostname := "hostname_" + strconv.Itoa(gofakeit.Number(0, maxNumberOfHosts))
			region := gofakeit.TimeZoneRegion()
			usageUser := gofakeit.Number(1, maxNumberOfUsageUsers)
			usageSystem := gofakeit.Number(1, maxNumberOfUsageSystem)
			streamData = NewDevops(table, hostname, region, int8(usageUser), int8(usageSystem), time.Now())
		default:
			device := "device_" + strconv.Itoa(gofakeit.Number(0, maxNumberOfDevices))
			number := gofakeit.Number(0, maxNumberIot)
			streamData = NewIot(table, device, int8(number), time.Now())
		}

		generatedData[i] = streamData.ToChangeItem(int64(i))
		metrics.Size.Add(int64(generatedData[i].Size.Read))
		metrics.Count.Inc()
	}

	return generatedData
}
func (s *Source) Stop() {
	s.cancel()
}

func NewSource(src *SampleSource, transferID string, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	metric := stats.NewSourceStats(registry)
	ctx, cancel := context.WithCancel(context.Background())

	return &Source{
		src:        src,
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metric,
		transferID: transferID,
		logger:     logger,
	}, nil
}
