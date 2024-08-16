package dblog

import (
	"strings"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
)

var supportedTypesArr = []string{
	"boolean",
	"bit",
	"varbit",

	"smallint",
	"smallserial",
	"integer",
	"serial",
	"bigint",
	"bigserial",
	"oid",

	"double precision",

	"char",
	"varchar",

	"character",
	"character varying",
	"timestamptz",
	"timestamp with time zone",
	"timetz",
	"time with time zone",
	"interval",

	"bytea",

	"jsonb",

	"uuid",

	"inet",
	"int4range",
	"int8range",
	"numrange",
	"tsrange",
	"tstzrange",
	"daterange",

	"float",
	"int",
	"text",

	"date",
	"time",

	"numeric",
	"decimal",
	"money",

	"cidr",
	"macaddr",
	"citext",
}

var supportedTypes = util.NewSet(supportedTypesArr...)

func IsSupportedKeyType(keyType string) bool {
	return supportedTypes.Contains(strings.TrimPrefix(keyType, "pg:"))
}
