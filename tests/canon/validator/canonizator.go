package validator

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

type CanonizatorSink struct {
	rows                  []abstract.ChangeItem
	t                     *testing.T
	commited              bool
	cntr                  int
	changeItemMiddlewares []func(items []abstract.ChangeItem) []abstract.ChangeItem
}

func RemoveVariableFieldsRowMiddleware(items []abstract.ChangeItem) []abstract.ChangeItem {
	for i := range items {
		items[i].LSN = 0
		items[i].CommitTime = 0
		items[i].TxID = ""
		items[i].ID = 0
	}
	return items
}

func (c *CanonizatorSink) Close() error {
	if c.commited {
		if len(c.rows) > 0 {
			logger.Log.Error("pushed rows list is not empty for commited sink")
		}
		c.commited = false
		return nil
	}

	c.t.Run(fmt.Sprintf("canon %v", c.cntr), func(t *testing.T) {
		for _, mw := range c.changeItemMiddlewares {
			c.rows = mw(c.rows)
		}
		if len(c.rows) > 0 {
			var typedChanges []abstract.TypedChangeItem
			for _, row := range c.rows {
				typedChanges = append(typedChanges, abstract.TypedChangeItem(row))
			}
			rawJSON, err := json.MarshalIndent(typedChanges, "", "    ")
			require.NoError(t, err)
			fmt.Println(string(rawJSON))
			canon.SaveJSON(t, string(rawJSON))
		}
	})
	c.cntr++
	return nil
}

func (c *CanonizatorSink) Push(items []abstract.ChangeItem) error {
	for _, row := range items {
		if !row.IsRowEvent() || row.IsSystemTable() {
			logger.Log.Info("non-row event presented")
			continue
		}
		c.rows = append(c.rows, row)
	}
	return nil
}

func (c *CanonizatorSink) Commit() error {
	c.commited = true
	return nil
}

func Canonizator(
	t *testing.T,
	changeItemMiddlewares ...func(item []abstract.ChangeItem) []abstract.ChangeItem,
) func() abstract.Sinker {
	return func() abstract.Sinker {
		return &CanonizatorSink{
			t:                     t,
			changeItemMiddlewares: append(changeItemMiddlewares, RemoveVariableFieldsRowMiddleware),
		}
	}
}
