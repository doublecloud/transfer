package dblog

import (
	"strings"

	"github.com/doublecloud/transfer/pkg/util/set"
)

var supportedTypesArr = []string{
	"boolean",
	"bit",

	"tinyint",
	"smallint",
	"mediumint",
	"int",
	"integer",
	"bigint",

	"double",
	"double precision",
	"float",
	"decimal",
	"numeric",

	"char",
	"varchar",
	"text",
	"tinytext",
	"mediumtext",
	"longtext",

	"binary",
	"varbinary",
	"blob",
	"tinyblob",
	"mediumblob",
	"longblob",

	"date",
	"datetime",
	"timestamp",
	"time",
	"year",

	"json",

	"uuid", // MySQL 8.0+

	"set",
	"enum",
}

var supportedTypes = set.New(supportedTypesArr...)

func IsSupportedKeyType(keyType string) bool {
	normalKeyType := strings.Split(keyType, "(")[0]
	return supportedTypes.Contains(strings.TrimPrefix(normalKeyType, "mysql:"))
}
