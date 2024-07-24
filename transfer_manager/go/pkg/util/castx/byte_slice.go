package castx

import "github.com/doublecloud/tross/library/go/core/xerrors"

// ToByteSliceE casts the type to a slice of bytes. The returned slice is a copy of the original one.
func ToByteSliceE(v any) ([]byte, error) {
	switch vCasted := v.(type) {
	case []byte:
		result := make([]byte, len(vCasted))
		copy(result, vCasted)
		return result, nil
	case string:
		return []byte(vCasted), nil
	default:
		return nil, NewCastError(xerrors.Errorf("no known conversion from %T to []byte", vCasted))
	}
}
