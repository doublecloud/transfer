package csv

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

// this function needed to check simultaneously: ReadAll() & ValidateOneLine() - they always should give the same result.
func csvReaderReadAll(t *testing.T, reader *Reader) ([][]string, error) {
	result, errExpected := reader.ReadAll()
	for _, el := range result {
		colNum, err := reader.ValidateOneLine(strings.Join(el, string(reader.Delimiter)))
		require.Equal(t, errExpected, err)
		require.Equal(t, len(el), colNum)
	}
	return result, errExpected
}

func TestReadAll(t *testing.T) {
	t.Run("simple example", func(t *testing.T) {
		content := bytes.NewBufferString(`1, 2, 3
		a,b,c
		7,8,9` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		// we expect 3 lines each with 3 elements

		require.Len(t, result, 3)
		require.Len(t, result[1], 3)
	})

	t.Run("newline is parsed as one single element", func(t *testing.T) {
		content := bytes.NewBufferString(`1, 2,   3," 4
		4    ", 5` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.NewlinesInValue = true

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Equal(t, "` 4\n\t\t4    `", result[0][3])
		require.Len(t, result, 1)
	})

	t.Run("quoted newline in data with false setting NewlinesInValue", func(t *testing.T) {
		content := bytes.NewBufferString("123123,\"2000-01-01\",\"\nmysuperdata\"")

		csvReader := NewReader(bufio.NewReader(content))
		csvReader.NewlinesInValue = false

		_, err := csvReaderReadAll(t, csvReader)
		require.Error(t, err)
	})

	t.Run("quoting is disallowed", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "c", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.QuoteChar = 0

		_, err := csvReaderReadAll(t, csvReader)

		require.ErrorIs(t, err, errQuotingDisabled)
	})

	t.Run("using uninitialized / invalid delimiter", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "c", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.Delimiter = 0

		_, err := csvReaderReadAll(t, csvReader)

		require.ErrorIs(t, err, errInvalidDelimiter)
	})

	t.Run("escape char outside of quotes treated as normal char", func(t *testing.T) {
		content := bytes.NewBufferString(`a, \, "c \" e , f"` + "\n")
		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		res, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Equal(t, "\\", res[0][1])
		require.Equal(t, `"c \" e , f"`, res[0][2])
		require.Len(t, res, 1)
	})

	t.Run("no escape char configured", func(t *testing.T) {
		content := bytes.NewBufferString(`a, \, "c \" e , f"` + "\n")

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		var escape rune
		csvReader.EscapeChar = escape

		res, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Equal(t, "\\", res[0][1])
		require.Equal(t, `"c \" e`, res[0][2])
		require.Equal(t, `f"`, res[0][3])

		require.Len(t, res, 1)
	})

	t.Run("double quoting is disallowed", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "the main ""test"" is this", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.DoubleQuote = false

		_, err := csvReaderReadAll(t, csvReader)
		require.Error(t, err)

		require.ErrorIs(t, err, errDoubleQuotesDisabled)
	})

	t.Run("double quoting is allowed", func(t *testing.T) {
		content := bytes.NewBufferString(`a, b, "the main ""test"" is this", d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.DoubleQuote = true

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)

		require.Equal(t, [][]string{{"a", "b", "`the main \"test\" is this`", "d"}}, result)
	})

	t.Run("using different delimiter", func(t *testing.T) {
		content := bytes.NewBufferString(`a; b; "c"; d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.Delimiter = ';'

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Equal(t, [][]string{{"a", "b", "\"c\"", "d"}}, result)
	})

	t.Run("using different quotes char", func(t *testing.T) {
		content := bytes.NewBufferString(`a, (b(, (c(, d` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)
		csvReader.QuoteChar = '('

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Equal(t, [][]string{{"a", "\"b\"", "\"c\"", "d"}}, result)
	})

	t.Run("delimiter in quotes is not used as separator", func(t *testing.T) {
		content := bytes.NewBufferString(`1, "2", "3 , 3", 4` + "\n")

		reader := bufio.NewReader(content)

		csvReader := NewReader(reader)

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Len(t, result[0], 4)
		require.Equal(t, [][]string{{"1", "\"2\"", "\"3 , 3\"", "4"}}, result)
	})

	t.Run("double quotes are converted to single quotes", func(t *testing.T) {
		content := bytes.NewBufferString(`1, ""2"", 3` + "\n")

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Len(t, result[0], 3)
		require.Equal(t, [][]string{{"1", "`\"2\"`", "3"}}, result)
	})

	t.Run("different encoding for string is still valid", func(t *testing.T) {
		encoder := charmap.ISO8859_1.NewEncoder()

		isoEncodedString, err := encoder.String(`1, ""äääää"", 3` + "\n")
		require.NoError(t, err)

		content := bytes.NewBufferString(isoEncodedString)

		reader := bufio.NewReader(content)
		csvReader := NewReader(reader)

		// try without encoding, see that string is broken
		result, err := csvReaderReadAll(t, csvReader)
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1", "`\"\xe4\xe4\xe4\xe4\xe4\"`", "3"}}, result)

		// if we set the encoding, its decoded correctly
		content2 := bytes.NewBufferString(isoEncodedString)
		reader2 := bufio.NewReader(content2)
		csvReader2 := NewReader(reader2)
		csvReader2.Encoding = charmap.ISO8859_1.String()

		result, err = csvReader2.ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1", "`\"äääää\"`", "3"}}, result)
	})
}
