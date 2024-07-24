package changeitem

import (
	"fmt"
)

type TxBound struct {
	Left, Right int
}

func TxBounds(batch []ChangeItem) []TxBound {
	txs := make([]TxBound, 0)
	if len(batch) == 0 {
		return txs
	}
	cID := batch[0].ID
	prevIDX := 0
	keys := map[string]bool{}
	for i := range batch {
		if cID != batch[i].ID {
			if batch[i].Kind == "update" && keys[fmt.Sprintf("%v", batch[i].OldKeys.KeyValues)] {
				txs = append(txs, TxBound{prevIDX, i})
				prevIDX = i
				keys = map[string]bool{}
			} else {
				keys[fmt.Sprintf("%v", batch[i].OldKeys.KeyValues)] = true
			}
		}
	}
	txs = append(txs, TxBound{prevIDX, len(batch)})
	return txs
}
