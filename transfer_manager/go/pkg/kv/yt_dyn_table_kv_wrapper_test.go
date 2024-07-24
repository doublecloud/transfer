package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestKvUsualUseCase(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	tmpPath := env.TmpPath()

	type kT struct {
		A int `yson:"a,key"`
		B int `yson:"b,key"`
	}

	type vT struct {
		C int `yson:"c"`
		D int `yson:"d"`
	}

	ctx := context.Background()

	ytDynTableKvWrapper, err := NewYtDynTableKVWrapper(
		ctx,
		env.YT,
		tmpPath,
		kT{},
		vT{},
		"default",
		map[string]interface{}{},
	)
	require.NoError(t, err)

	err = ytDynTableKvWrapper.InsertRow(ctx, kT{A: 1, B: 2}, vT{C: 3, D: 4})
	require.NoError(t, err)

	count, err := ytDynTableKvWrapper.CountAllRows(ctx)
	require.NoError(t, err)
	require.Equal(t, count, uint64(1))

	err = ytDynTableKvWrapper.InsertRow(ctx, kT{A: 2, B: 3}, vT{C: 4, D: 5})
	require.NoError(t, err)

	count2, err := ytDynTableKvWrapper.CountAllRows(ctx)
	require.NoError(t, err)
	require.Equal(t, count2, uint64(2))

	tx, err := env.YT.BeginTabletTx(ctx, nil)
	require.NoError(t, err)
	lastKey := kT{A: 3, B: 4}
	lastVal := vT{C: 5, D: 6}
	err = ytDynTableKvWrapper.InsertRowsTx(ctx, tx, []interface{}{lastKey}, []interface{}{lastVal})
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	count3, err := ytDynTableKvWrapper.CountAllRows(ctx)
	require.NoError(t, err)
	require.Equal(t, count3, uint64(3))

	found, output, err := ytDynTableKvWrapper.GetValueByKey(ctx, lastKey)
	require.NoError(t, err)
	require.Equal(t, found, true)
	val := output.(*vT)
	require.Equal(t, val, &lastVal)
}
