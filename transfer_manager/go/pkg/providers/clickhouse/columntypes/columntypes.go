package columntypes

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
)

var numbersExtractorRe = regexp.MustCompile("[0-9]+")
var intRe = regexp.MustCompile(`^U?Int(\d{1,3})$`)

type TypeDescription struct {
	typ          string
	IsString     bool
	IsDecimal    bool
	IsArray      bool
	IsDateTime64 bool
	IsDateTime   bool
	IsDate       bool
	YtBaseType   string
	IsInteger    bool
}

type TypeMapping map[string]*TypeDescription

func NewTypeDescription(chTypeName string) *TypeDescription {
	baseType := BaseType(chTypeName)
	ytType, _ := ToYtType(baseType)
	return &TypeDescription{
		typ:          chTypeName,
		IsDateTime64: baseType == "DateTime64",
		IsDateTime:   baseType == "DateTime",
		IsDate:       baseType == "Date",
		IsString:     baseType == "String",
		IsArray:      strings.Contains(baseType, "Array"),
		IsDecimal:    baseType == "Decimal",
		IsInteger:    intRe.MatchString(baseType),
		YtBaseType:   ytType,
	}
}

func (t *TypeDescription) DateTime64Precision() int {
	if !t.IsDateTime64 {
		return 0
	}
	// We are looking for second number in string (first always 64)
	parts := numbersExtractorRe.FindAllString(t.typ, 2)
	if len(parts) < 2 {
		return 0
	}
	res, _ := strconv.Atoi(parts[1])
	return res
}

func LegacyIsDecimal(column abstract.ColSchema) bool {
	// Old behavior for homo transfers
	return strings.HasPrefix(column.OriginalType, "ch:Decimal(") ||
		strings.HasPrefix(column.OriginalType, "ch:Nullable(Decimal(")
}
