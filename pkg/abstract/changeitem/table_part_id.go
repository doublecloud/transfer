package changeitem

import (
	"fmt"
)

// TablePartID describes table part for sharded upload.
// Sinker may use this type to distinguish different upload parts of different tables.
type TablePartID struct {
	TableID
	// PartID is the same as ChangeItem.PartID
	PartID string
}

func (t *TablePartID) FqtnWithPartID() string {
	if t.PartID == "" {
		return t.TableID.Fqtn()
	}
	return fmt.Sprintf("%s.%s", t.TableID.Fqtn(), t.PartID)
}
