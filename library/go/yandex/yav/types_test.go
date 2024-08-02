package yav

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimestamp_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name            string
		input           []byte
		expectTimestamp Timestamp
		expectError     error
	}{
		{
			"empty_bytes",
			nil,
			Timestamp{},
			&strconv.NumError{Func: "ParseInt", Num: "", Err: errors.New("invalid syntax")},
		},
		{
			"non_timestamp",
			[]byte("ololo"),
			Timestamp{},
			&strconv.NumError{Func: "ParseInt", Num: "ololo", Err: errors.New("invalid syntax")},
		},
		{
			"bad_fraction",
			[]byte("1564828284.trololo"),
			Timestamp{},
			&strconv.NumError{Func: "ParseInt", Num: "trololo", Err: errors.New("invalid syntax")},
		},
		{
			"without_fraction",
			[]byte("1564828284"),
			Timestamp{Time: time.Unix(1564828284, 0)},
			nil,
		},
		{
			"with_fraction",
			[]byte("1564828284.42"),
			Timestamp{Time: time.Unix(1564828284, 42)},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var ts Timestamp
			err := ts.UnmarshalJSON(tc.input)

			if tc.expectError != nil {
				assert.Equal(t, tc.expectError, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expectTimestamp, ts)
		})
	}
}
