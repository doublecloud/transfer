package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

func TestFitTimeToYT(t *testing.T) {
	ytMinTime, _ := time.Parse(time.RFC3339Nano, "1970-01-01T00:00:00.000000Z")
	ytMaxTime, _ := time.Parse(time.RFC3339Nano, "2105-12-31T23:59:59.999999Z")

	underMinTime := ytMinTime.Add(-time.Hour * 240)
	_, err := yt_schema.NewTimestamp(underMinTime)
	newTime, err := FitTimeToYT(underMinTime, err)
	require.NoError(t, err)
	require.Equal(t, ytMinTime, newTime)

	overMaxTime := ytMaxTime.Add(time.Hour * 240)
	_, err = yt_schema.NewTimestamp(overMaxTime)
	newTime, err = FitTimeToYT(overMaxTime, err)
	require.NoError(t, err)
	require.Equal(t, ytMaxTime, newTime)

	now := time.Now()
	newTime, err = FitTimeToYT(now, nil)
	require.NoError(t, err)
	require.Equal(t, now, newTime)
}
