package reader

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/changeitem"
	"github.com/valyala/fastjson/fastfloat"
	"go.ytsaurus.tech/yt/go/schema"
)

func (r *CSVReader) fillVals(vals []interface{}, row []string, valIDX int, rowIDX int, col changeitem.ColSchema) error {
	var ok bool
	switch col.DataType {
	case schema.TypeBoolean.String():
		vals[valIDX], ok = r.parseBooleanValue(row[rowIDX])
		if !ok {
			return xerrors.Errorf("unable to parse bool: %s", row[rowIDX])
		}
	case schema.TypeDate.String(), schema.TypeDatetime.String():
		vals[valIDX], ok = r.parseDateValue(row[rowIDX])
		if !ok {
			return xerrors.Errorf("unable to parse date value: %s", row[rowIDX])
		}
	case schema.TypeTimestamp.String():
		vals[valIDX], ok = r.parseTimestampValue(row[rowIDX])
		if !ok {
			return xerrors.Errorf("unable to parse timestamp value: %s", row[rowIDX])
		}
	case schema.TypeFloat32.String(), schema.TypeFloat64.String():
		vals[valIDX], ok = r.parseFloatValue(row[rowIDX])
		if !ok {
			return xerrors.Errorf("unable to parse timestamp value: %s", row[rowIDX])
		}
	default:
		if r.nullable() {
			vals[valIDX] = r.parseNullValues(row[rowIDX], col)
		}
		vals[valIDX] = row[rowIDX]
	}
	return nil
}

// parseFloatValue checks if the provided value can be correctly parsed to a float value
// if the specified decimal char is used.
// It defaults to the value itself if this converison is not possible.
func (r *CSVReader) parseFloatValue(originalValue string) (json.Number, bool) {
	if r.additionalReaderOptions.DecimalPoint != "" {
		possibleFloat := strings.Replace(originalValue, r.additionalReaderOptions.DecimalPoint, ".", 1)
		_, err := strconv.ParseFloat(possibleFloat, 64)
		if err == nil {
			return json.Number(possibleFloat), true
		} else {
			return "", false
		}
	} else {
		_, err := fastfloat.Parse(originalValue)
		if err == nil {
			return json.Number(originalValue), true
		}
		return "", false
	}
}

func (r *CSVReader) nullable() bool {
	return r.additionalReaderOptions.QuotedStringsCanBeNull || r.additionalReaderOptions.StringsCanBeNull
}

// parseNullValues checks if the provided value is part of the null values list provided by the user.
// If this is the case the zero value of the datatype is returned for this value.
// It defaults to the original value if the value is not contained in the list or if the  conditions of the
// boolean flags (QuotedStringsCanBeNull, StringsCanBeNull) are not fulfilled.
func (r *CSVReader) parseNullValues(originalValue string, col abstract.ColSchema) interface{} {
	if r.additionalReaderOptions.QuotedStringsCanBeNull {
		trimmedContent := originalValue
		if strings.HasPrefix(originalValue, "\"") && strings.HasSuffix(originalValue, "\"") {
			trimmedContent = strings.TrimSuffix(strings.TrimPrefix(originalValue, "\""), "\"")
		} else if strings.HasPrefix(originalValue, "'") && strings.HasSuffix(originalValue, "'") {
			trimmedContent = strings.TrimSuffix(strings.TrimPrefix(originalValue, "'"), "'")
		}
		if contains(r.additionalReaderOptions.NullValues, trimmedContent) {
			return abstract.DefaultValue(&col)
		}
	} else {
		if r.additionalReaderOptions.StringsCanBeNull {
			if contains(r.additionalReaderOptions.NullValues, originalValue) {
				return abstract.DefaultValue(&col)
			}
		}
	}

	return originalValue
}

// parseDateValue checks if the provided value can be parsed to a time.Time through one of
// the user provided TimestampParsers. It defaults to the original value if this is not the case.
func (r *CSVReader) parseDateValue(originalValue string) (time.Time, bool) {
	if len(r.additionalReaderOptions.TimestampParsers) == 0 {
		res, _ := dateparse.ParseAny(originalValue)
		return res, true
	}
	for _, parser := range r.additionalReaderOptions.TimestampParsers {
		dateValue, err := time.Parse(parser, originalValue)
		if err == nil {
			return dateValue, true
		}
	}
	return time.Time{}, false
}

// parseTimestampValue checks if the provided value can be parsed to a time.Time.
// It defaults to the original value if this is not the case.
func (r *CSVReader) parseTimestampValue(originalValue string) (time.Time, bool) {
	toInt64, err := strconv.ParseInt(originalValue, 10, 64)
	if err == nil {
		return time.Unix(toInt64, 0), true
	}

	return time.Time{}, false
}

// parseBooleanValue checks if the provided value is contained in one of the provided lists of
// true/false values and returns the corresponding boolean value. If the value is contained in the null values
// then a false boolean value is returned for this value. It defaults to the original value if no matches are found.
func (r *CSVReader) parseBooleanValue(originalValue string) (bool, bool) {
	if r.additionalReaderOptions.StringsCanBeNull {
		if contains(r.additionalReaderOptions.NullValues, originalValue) {
			return false, true
		}
	}
	if contains(r.additionalReaderOptions.TrueValues, originalValue) {
		return true, true
	} else if contains(r.additionalReaderOptions.FalseValues, originalValue) {
		return false, true
	} else {
		// last ditch attempt, try string conversion
		boolVal, err := strconv.ParseBool(originalValue)
		if err != nil {
			return false, false
		}
		return boolVal, true
	}
}
