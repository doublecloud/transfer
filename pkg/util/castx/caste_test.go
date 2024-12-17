package castx

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDurationToIso8601(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		testsData := [][2]string{
			{"1s", "PT1S"},
			{"1m", "PT1M"},
			{"1h", "PT1H"},
			{"24h", "P1D"},
			{"1ms", "P0D"},
			{"1000ms", "PT1S"},
			{fmt.Sprint(115*24*time.Hour + 17*time.Hour + 46*time.Minute + 39*time.Second), "P115DT17H46M39S"},
		}

		for _, data := range testsData {
			input, expected := data[0], data[1]
			d, err := time.ParseDuration(input)
			require.NoError(t, err)
			actual, err := DurationToIso8601(d)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}
	})

	t.Run("error", func(t *testing.T) {
		d, err := time.ParseDuration("-1h")
		require.NoError(t, err)
		_, err = DurationToIso8601(d)
		require.Error(t, err)
	})
}
