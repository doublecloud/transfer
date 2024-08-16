package ydb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil"
)

func TestOperationParams(t *testing.T) {
	for _, test := range [...]struct {
		name string

		ctxTimeout time.Duration

		opTimeout time.Duration
		opCancel  time.Duration
		opMode    OperationMode

		exp OperationParams
	}{
		{
			name: "nothing",
		},
		{
			name:       "mode: unknown, context timeout",
			ctxTimeout: time.Second,
			exp:        OperationParams{},
		},
		{
			name:       "mode: sync, context timeout applied to operation timeout",
			ctxTimeout: time.Second,
			opMode:     OperationModeSync,
			exp: OperationParams{
				Timeout: time.Second,
				Mode:    OperationModeSync,
			},
		},
		{
			name:       "mode: async, context timeout not applied to operation timeout",
			ctxTimeout: time.Second,
			opMode:     OperationModeAsync,
			exp: OperationParams{
				Mode: OperationModeAsync,
			},
		},
		{
			name:       "mode: unknown, context timeout not override operation timeout",
			ctxTimeout: time.Second,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Hour,
			},
		},
		{
			name:       "mode: sync, context timeout override operation timeout",
			ctxTimeout: time.Second,
			opMode:     OperationModeSync,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Second,
				Mode:    OperationModeSync,
			},
		},
		{
			name:       "mode: async, context timeout not override operation timeout",
			ctxTimeout: time.Second,
			opMode:     OperationModeAsync,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Hour,
				Mode:    OperationModeAsync,
			},
		},
		{
			name:       "mode: unknown, cancel after timeout",
			ctxTimeout: time.Second,
			opCancel:   time.Hour,
			exp: OperationParams{
				CancelAfter: time.Hour,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
			defer cleanupNow()

			ctx := context.Background()
			if t := test.opTimeout; t > 0 {
				ctx = WithOperationTimeout(ctx, t)
			}
			if t := test.opCancel; t > 0 {
				ctx = WithOperationCancelAfter(ctx, t)
			}
			if m := test.opMode; m != 0 {
				ctx = WithOperationMode(ctx, m)
			}
			if t := test.ctxTimeout; t > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, timeutil.Now().Add(t))
				defer cancel()
			}

			act := operationParams(ctx)

			if exp := test.exp; !reflect.DeepEqual(act, exp) {
				t.Fatalf(
					"unexpected operation parameters: %v; want %v",
					act, exp,
				)
			}
		})
	}
}

func TestContextOperationMode(t *testing.T) {

}
