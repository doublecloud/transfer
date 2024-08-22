package mysql

import _ "embed"

var (
	//go:embed dump/initial_data.sql
	initial []byte

	//go:embed dump/date_types.sql
	date_ []byte
	//go:embed dump/json_types.sql
	json_ []byte
	//go:embed dump/numeric_types_bit.sql
	numericBit_ []byte
	//go:embed dump/numeric_types_boolean.sql
	numericBoolean_ []byte
	//go:embed dump/numeric_types_decimal.sql
	numericDecimal_ []byte
	//go:embed dump/numeric_types_float.sql
	numericFloat_ []byte
	//go:embed dump/numeric_types_int.sql
	numericInt_ []byte
	//go:embed dump/spatial_types.sql
	spatial_ []byte
	//go:embed dump/string_types.sql
	string_ []byte
	//go:embed dump/string_types_emoji.sql
	stringEmoji_ []byte
)

var (
	TableSQLs = map[string]string{
		"initial":               string(initial),
		"date_types":            string(date_),
		"json_types":            string(json_),
		"numeric_types_bit":     string(numericBit_),
		"numeric_types_boolean": string(numericBoolean_),
		"numeric_types_decimal": string(numericDecimal_),
		"numeric_types_float":   string(numericFloat_),
		"numeric_types_int":     string(numericInt_),
		"spatial_types":         string(spatial_),
		"string_types":          string(string_),
		"string_types_emoji":    string(stringEmoji_),
	}
)
