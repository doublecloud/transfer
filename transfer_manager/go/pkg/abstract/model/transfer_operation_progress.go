package model

import "time"

type AggregatedProgress struct {
	PartsCount          int64
	CompletedPartsCount int64
	ETARowsCount        int64
	CompletedRowsCount  int64
	TotalReadBytes      int64
	TotalDuration       time.Duration
	LastUpdateAt        time.Time
}

func (a *AggregatedProgress) Empty() bool {
	return a == nil || a.PartsCount == 0
}

func (a *AggregatedProgress) PartsPercent() float64 {
	if a == nil || a.PartsCount == 0 {
		return 0
	}

	return (float64(a.CompletedPartsCount) / float64(a.PartsCount)) * 100
}

func (a *AggregatedProgress) RowsPercent() float64 {
	if a == nil || a.ETARowsCount == 0 {
		return 0
	}

	return (float64(a.CompletedRowsCount) / float64(a.ETARowsCount)) * 100
}

func NewAggregatedProgress() *AggregatedProgress {
	return &AggregatedProgress{
		PartsCount:          0,
		CompletedPartsCount: 0,
		ETARowsCount:        0,
		CompletedRowsCount:  0,
		TotalReadBytes:      0,
		TotalDuration:       0,
		LastUpdateAt:        time.Now(),
	}
}
