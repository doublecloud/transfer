package reader

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/s3"
	"github.com/stretchr/testify/require"
)

func TestParseFloatValue(t *testing.T) {
	require := require.New(t)

	r := &CSVReader{additionalReaderOptions: s3.AdditionalOptions{DecimalPoint: ","}}

	// Test case 1: Valid float value with DecimalPoint "," original value is changed
	originalValue := "123,456"
	expected := json.Number("123.456")
	result, ok := r.parseFloatValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 1 failed")

	// Test case 2: Valid float value with DecimalPoint "." nothing to change
	r.additionalReaderOptions.DecimalPoint = "."
	originalValue = "123.456"
	expected = "123.456"
	result, ok = r.parseFloatValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 2 failed")

	// Test case 3: Invalid float value, original value is kept
	originalValue = "abc"
	_, ok = r.parseFloatValue(originalValue)
	require.False(ok)

	// Test case 4: No DecimalPoint set original value is kept
	r.additionalReaderOptions.DecimalPoint = ""
	originalValue = "123.456"
	expected = "123.456"
	result, ok = r.parseFloatValue(originalValue)
	require.Equal(expected, result, "Test case 4 failed")
	require.True(ok)
}

func TestParseNullValues(t *testing.T) {
	require := require.New(t)

	r := &CSVReader{
		additionalReaderOptions: s3.AdditionalOptions{
			StringsCanBeNull:       true,
			QuotedStringsCanBeNull: true,
			NullValues:             []string{"NULL", "NA"},
		},
	}

	// Test case 1: Original value is a quoted string and a null value
	originalValue := "\"NULL\""
	col := abstract.ColSchema{} // empty column schema for demonstration
	expected := abstract.DefaultValue(&col)
	result := r.parseNullValues(originalValue, col)
	require.Equal(expected, result, "Test case 1 failed")

	// Test case 2: Original value is a quoted string but not a null value
	originalValue = "\"notnull\""
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(expected, result, "Test case 2 failed")

	// Test case 3: Original value is not a quoted string but a null value
	originalValue = "NULL"
	expected = abstract.DefaultValue(&col)
	result = r.parseNullValues(originalValue, col)
	require.Equal(expected, result, "Test case 3 failed")

	// Test case 4: Original value is not a quoted string and not a null value
	originalValue = "notnull"
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(expected, result, "Test case 4 failed")

	// Test case 5: StringsCanBeNull and QuotedStringsCanBeNull are both false
	r.additionalReaderOptions.StringsCanBeNull = false
	r.additionalReaderOptions.QuotedStringsCanBeNull = false
	originalValue = "\"NULL\""
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(expected, result, "Test case 5 failed")

	// Test case 6: Original value is not in the NullValues list
	r.additionalReaderOptions.StringsCanBeNull = true
	r.additionalReaderOptions.QuotedStringsCanBeNull = true
	originalValue = "notnull"
	expected = originalValue
	result = r.parseNullValues(originalValue, col)
	require.Equal(expected, result, "Test case 6 failed")
}

func TestParseDateValue(t *testing.T) {
	require := require.New(t)

	r := &CSVReader{
		additionalReaderOptions: s3.AdditionalOptions{
			TimestampParsers: []string{
				"2006-01-02",      // yyyy-mm-dd
				"02-Jan-2006",     // dd-Mon-yyyy
				"January 2, 2006", // Month dd, yyyy
			},
		},
	}

	// Test case 1: Original value can be parsed with the first timestamp parser
	originalValue := "2024-03-22"
	expected, _ := time.Parse("2006-01-02", originalValue)
	result, ok := r.parseDateValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 1 failed")

	// Test case 2: Original value can be parsed with the second timestamp parser
	originalValue = "22-Mar-2024"
	expected, _ = time.Parse("02-Jan-2006", originalValue)
	result, ok = r.parseDateValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 2 failed")

	// Test case 3: Original value can be parsed with the third timestamp parser
	originalValue = "March 22, 2024"
	expected, _ = time.Parse("January 2, 2006", originalValue)
	result, ok = r.parseDateValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 3 failed")

	// Test case 4: Original value cannot be parsed with any timestamp parser
	originalValue = "2024/03/22"
	_, ok = r.parseDateValue(originalValue)
	require.False(ok)
}

func TestParseBooleanValue(t *testing.T) {
	require := require.New(t)

	r := &CSVReader{
		additionalReaderOptions: s3.AdditionalOptions{
			StringsCanBeNull: true,
			NullValues:       []string{"NULL", "NA"},
			TrueValues:       []string{"true", "yes", "1"},
			FalseValues:      []string{"false", "no", "0"},
		},
	}

	// Test case 1: Original value is a null value
	originalValue := "NULL"
	expected := false
	result, ok := r.parseBooleanValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 1 failed")

	// Test case 2: Original value is a true value
	originalValue = "true"
	expected = true
	result, ok = r.parseBooleanValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 2 failed")

	// Test case 3: Original value is a false value
	originalValue = "false"
	expected = false
	result, ok = r.parseBooleanValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 3 failed")

	// Test case 4: Original value is not in any of the true/false/null values lists, but can be parsed as boolean
	originalValue = "TRUE"
	expected = true
	result, ok = r.parseBooleanValue(originalValue)
	require.True(ok)
	require.Equal(expected, result, "Test case 4 failed")

	// Test case 5: Original value is not in any of the true/false/null values lists and cannot be parsed as boolean
	originalValue = "random"
	_, ok = r.parseBooleanValue(originalValue)
	require.False(ok)
}
