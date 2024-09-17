package postgres

import (
	"testing"

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
