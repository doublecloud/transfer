package castx

import (
	"encoding/json"

	"github.com/doublecloud/tross/library/go/core/xerrors"
)

func ToJSONMarshallableE[T any](v T) (T, error) {
	if _, err := json.Marshal(v); err != nil {
		return v, NewCastError(xerrors.Errorf("%T is not a JSON marshallable type: %w", v, err))
	}
	return v, nil
}
