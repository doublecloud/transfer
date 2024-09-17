package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewXDArray(t *testing.T) {
	array, err := NewXDArray([]int{0})
	require.NoError(t, err)
	require.Equal(t, []interface{}{}, array.Data)

	array, err = NewXDArray([]int{0, 0})
	require.NoError(t, err)
	require.Equal(t, []interface{}{}, array.Data)

	array, err = NewXDArray([]int{1})
	require.NoError(t, err)
	require.Equal(t, make([]interface{}, 1), array.Data)

	array, err = NewXDArray([]int{3})
	require.NoError(t, err)
	require.Equal(t, make([]interface{}, 3), array.Data)

	array, err = NewXDArray([]int{1, 1, 1})
	require.NoError(t, err)
	require.Equal(t, []interface{}{[]interface{}{make([]interface{}, 1)}}, array.Data)

	array, err = NewXDArray([]int{2, 3})
	require.NoError(t, err)
	require.Equal(t, []interface{}{make([]interface{}, 3), make([]interface{}, 3)}, array.Data)

	array = EmptyXDArray()
	require.Equal(t, []interface{}{}, array.Data)
}

func TestNewXDArrayError(t *testing.T) {
	_, err := NewXDArray(nil)
	require.Error(t, err)

	_, err = NewXDArray([]int{})
	require.Error(t, err)

	_, err = NewXDArray([]int{-1})
	require.Error(t, err)

	_, err = NewXDArray([]int{1, -1})
	require.Error(t, err)
}

func TestFullSize(t *testing.T) {
	fullSize := FullSize([]int{2, 3, 4})
	require.Equal(t, []int{12, 4, 1}, fullSize)
}

func TestXDIndex(t *testing.T) {
	fullSize := FullSize([]int{3})
	require.Equal(t, []int{0}, XDIndex(0, fullSize))
	require.Equal(t, []int{1}, XDIndex(1, fullSize))
	require.Equal(t, []int{2}, XDIndex(2, fullSize))

	fullSize = FullSize([]int{2, 2})
	require.Equal(t, []int{0, 0}, XDIndex(0, fullSize))
	require.Equal(t, []int{0, 1}, XDIndex(1, fullSize))
	require.Equal(t, []int{1, 0}, XDIndex(2, fullSize))
	require.Equal(t, []int{1, 1}, XDIndex(3, fullSize))
}

func TestXDArray_Set(t *testing.T) {
	size := []int{3}
	array, err := NewXDArray(size)
	require.NoError(t, err)
	fullSize := FullSize(size)
	for i := 0; i < 3; i++ {
		index := XDIndex(i, fullSize)
		require.NoError(t, array.Set(index, i+1))
	}
	require.Equal(t, []interface{}{1, 2, 3}, array.Data)

	size = []int{2, 3}
	array, err = NewXDArray(size)
	require.NoError(t, err)
	fullSize = FullSize(size)
	for i := 0; i < 6; i++ {
		index := XDIndex(i, fullSize)
		require.NoError(t, array.Set(index, i+1))
	}
	require.Equal(t, []interface{}{[]interface{}{1, 2, 3}, []interface{}{4, 5, 6}}, array.Data)
}
