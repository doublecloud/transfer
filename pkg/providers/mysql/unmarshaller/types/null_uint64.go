package types

import (
	"database/sql"
	"database/sql/driver"
	"strconv"

	"golang.org/x/xerrors"
)

type NullUint64 struct {
	UInt64 uint64
	Valid  bool // Valid is true if Int64 is not NULL
}

var _ driver.Valuer = (*NullUint64)(nil)
var _ sql.Scanner = (*NullUint64)(nil)

func (n *NullUint64) Scan(value interface{}) error {
	n.UInt64 = uint64(0)
	n.Valid = false

	if value == nil {
		n.Valid = false
		return nil
	}

	var source *[]byte
	switch t := value.(type) {
	case []byte:
		source = &t
	case *[]byte:
		source = t
	}

	if source != nil {
		v, err := strconv.ParseUint(string(*source), 10, 64)
		if err != nil {
			n.Valid = false
			return xerrors.Errorf("unable to parse unsigned int: %w", err)
		}
		n.Valid = true
		n.UInt64 = v
	}

	return nil
}

func (n *NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.UInt64, nil
}
