package sample

import (
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
)

const (
	iotSampleType = "iot"
)

var (
	_ server.Source = (*SampleSource)(nil)
)

type SampleSource struct {
	SampleType    string
	TableName     string
	MaxSampleData int
	MinSleepTime  time.Duration
}

func (s *SampleSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *SampleSource) Validate() error {
	return nil
}

func (s *SampleSource) WithDefaults() {
	if s.SampleType == "" {
		s.SampleType = iotSampleType
	}
	if s.MaxSampleData == 0 {
		s.MaxSampleData = 10
	}
	if s.MinSleepTime == 0 {
		s.MinSleepTime = 300 * time.Millisecond
	}
	if s.TableName == "" {
		s.TableName = s.SampleType
	}
}

func (s *SampleSource) IsSource() {}
