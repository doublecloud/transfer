package glob

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testGlobMatch(t *testing.T, pattern, subj string) {
	if !Match(pattern, subj) {
		t.Fatalf("%s should match %s", pattern, subj)
	}
}

func testGlobNoMatch(t *testing.T, pattern, subj string) {
	if Match(pattern, subj) {
		t.Fatalf("%s should not match %s", pattern, subj)
	}
}

func TestEmptyPattern(t *testing.T) {
	testGlobMatch(t, "", "")
	testGlobNoMatch(t, "", "test")
}

func TestSplitMatch(t *testing.T) {
	require.True(t, SplitMatch("*2023*|*2022-12*", "yellow_taxi/yellow_tripdata_2023-01.parquet", "|"))
	require.True(t, SplitMatch("*2023*|*2022-12*", "yellow_taxi/yellow_tripdata_2022-12.parquet", "|"))
	require.False(t, SplitMatch("*2023*|*2022-12*", "yellow_taxi/yellow_tripdata_2022-07.parquet", "|"))
}

func TestEmptySubject(t *testing.T) {
	for _, pattern := range []string{
		"",
		"*",
		"**",
		"***",
		"****************",
		strings.Repeat("*", 1000000),
	} {
		testGlobMatch(t, pattern, "")
	}

	for _, pattern := range []string{
		// No globs/non-glob characters
		"test",
		"*test*",

		// Trailing characters
		"*x",
		"*****************x",
		strings.Repeat("*", 1000000) + "x",

		// Leading characters
		"x*",
		"x*****************",
		"x" + strings.Repeat("*", 1000000),

		// Mixed leading/trailing characters
		"x*x",
		"x****************x",
		"x" + strings.Repeat("*", 1000000) + "x",
	} {
		testGlobNoMatch(t, pattern, "")
	}
}

func TestPatternWithoutGlobs(t *testing.T) {
	testGlobMatch(t, "test", "test")
}

func TestGlob(t *testing.T) {
	// Matches
	for _, pattern := range []string{
		"*test",           // Leading glob
		"this*",           // Trailing glob
		"this*test",       // Middle glob
		"*is *",           // String in between two globs
		"*is*a*",          // Lots of globs
		"**test**",        // Double glob characters
		"**is**a***test*", // Varying number of globs
		"* *",             // White space between globs
		"*",               // Lone glob
		"**********",      // Nothing but globs
		"*Ѿ*",             // Unicode with globs
		"*is a ϗѾ *",      // Mixed ASCII/unicode
	} {
		testGlobMatch(t, pattern, "this is a ϗѾ test")
	}

	// Non-matches
	for _, pattern := range []string{
		"test*",               // Implicit substring match
		"*is",                 // Partial match
		"*no*",                // Globs without a match between them
		" ",                   // Plain white space
		"* ",                  // Trailing white space
		" *",                  // Leading white space
		"*ʤ*",                 // Non-matching unicode
		"this*this is a test", // Repeated prefix
	} {
		testGlobNoMatch(t, pattern, "this is a test")
	}
}

func BenchmarkGlob(b *testing.B) {
	benchCases := []struct {
		name    string
		pattern string
	}{
		{"empty", ""},
		{"equal", "The quick brown fox jumped over the lazy dog"},
		{"not_equal", "The slow orange fox rolled under the crazy dog"},
		{"single_glob", "*"},
		{"consecutive_globs", "**"},
		{"multiple_consecutive_globs", "**********"},
		{"leading_match", "*brown fox jumped over the lazy dog"},
		{"trailing_match", "The quick brown fox jumped*"},
		{"middle_match", "The quick brown * jumped over the lazy dog"},
		{"mixed_match", "*quick brown * jumped*the lazy*"},
		{"leading_mismatch", "*orange fox rolled under the crazy dog"},
		{"trailing_mismatch", "The slow orange fox rolled*"},
		{"middle_mismatch", "The slow orange * rolled under the crazy dog"},
		{"mixed_mismatch", "*orange brown * rolled*the lazy*"},
		{"mixed_consecutive_globs", "The **brown fox*****jumped ov*** the*lazy dog"},
	}

	subj := "The quick brown fox jumped over the lazy dog"

	b.Run("match", func(b *testing.B) {
		for _, bc := range benchCases {
			b.Run(bc.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					Match(bc.pattern, subj)
				}
			})
		}
	})
}
