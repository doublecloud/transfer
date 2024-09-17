package scanner

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

var testDataLineBreak = []string{
	"lajsdhjlna;ksdjvn;kadnv",
	"ajlklj23576p9",
	"\\\r\r\t\fs\a\b\"",
	"9yct 2=0q3	-0w	dl1≥xr",
}

func TestLineBreakScanner(t *testing.T) {
	buf := bytes.Buffer{}
	for _, item := range testDataLineBreak {
		buf.WriteString(item)
		buf.WriteRune('\n')
	}

	sc := NewLineBreakScanner(buf.Bytes())

	_, err := sc.Event()
	require.Error(t, err)

	var got []string
	for range testDataLineBreak {
		require.True(t, sc.Scan())
		evt, err := sc.Event()
		require.NoError(t, err)
		got = append(got, string(evt))
	}
	require.False(t, sc.Scan())
	require.Equal(t, testDataLineBreak, got)

	multilineString := `this is the first line
this is the second
this is the third
this is the forth`

	scanner := NewLineBreakScanner([]byte(multilineString))
	splittedLines, err := scanner.ScanAll()
	require.NoError(t, err)
	require.Len(t, splittedLines, 4)
	require.Equal(t, []string{"this is the first line", "this is the second", "this is the third", "this is the forth"}, splittedLines)
}
