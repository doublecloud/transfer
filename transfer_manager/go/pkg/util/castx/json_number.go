package castx

import (
	"encoding/json"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/spf13/cast"
)

func ToJSONNumberE(v any) (json.Number, error) {
	vStr, err := cast.ToStringE(v)
	if err != nil {
		return json.Number("0"), NewCastError(err)
	}
	result := json.Number(vStr)
	if _, floatParseErr := result.Float64(); floatParseErr == nil {
		return result, nil
	}
	if _, intParseErr := result.Int64(); intParseErr == nil {
		return result, nil
	}
	return json.Number("0"), NewCastError(xerrors.Errorf("%s is not a parsable JSON number", vStr))
}
