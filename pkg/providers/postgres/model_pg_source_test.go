package postgres

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestPgDumpStepsAnyStepIsTrue(t *testing.T) {
	steps := PgDumpSteps{}
	require.False(t, steps.AnyStepIsTrue())

	sequenceSet := false
	steps.SequenceSet = &sequenceSet
	require.False(t, steps.AnyStepIsTrue())

	steps.Type = true
	require.True(t, steps.AnyStepIsTrue())
}

func TestIncludeEmptyTable(t *testing.T) {
	src := PgSource{
		DBTables:       []string{"myspace.*"},
		ExcludedTables: []string{"myspace.mytable"},
	}
	src.WithDefaults()
	require.NoError(t, src.Validate())
	require.True(t, src.Include(*abstract.NewTableID("myspace", "")))
}
