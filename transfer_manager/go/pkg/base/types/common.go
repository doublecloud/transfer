package types

import (
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

// nolint:descriptive_errors
func FitTimeToYT(value time.Time, ytTimeErr error) (time.Time, error) {
	var rangeErr = new(yt_schema.RangeError)
	if !xerrors.As(ytTimeErr, &rangeErr) && !xerrors.As(ytTimeErr, rangeErr) {
		//nolint:descriptiveerrors
		return value, ytTimeErr
	}

	minTime, minOk := rangeErr.MinValue.(time.Time)
	maxTime, maxOk := rangeErr.MaxValue.(time.Time)
	if !(minOk && maxOk) {
		return value, xerrors.Errorf(
			"Time value '%v' is out of range ['%v', '%v'] of unknown type", value, minTime, maxTime)
	}

	if value.Before(minTime) {
		return minTime, nil
	}

	if value.After(maxTime) {
		return maxTime, nil
	}

	//nolint:descriptiveerrors
	return value, ytTimeErr
}
