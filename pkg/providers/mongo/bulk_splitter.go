package mongo

import (
	"github.com/doublecloud/transfer/pkg/util"
	"go.mongodb.org/mongo-driver/mongo"
)

type bulkSplitter struct {
	bulks              [][]mongo.WriteModel
	currentOperations  []mongo.WriteModel
	currentDocumentIDs util.Set[string]
}

func (b *bulkSplitter) Add(operation mongo.WriteModel, id documentID, isolated bool) {
	if b.currentContains(id) || isolated {
		b.flush()
	}
	b.currentOperations = append(b.currentOperations, operation)
	b.currentDocumentIDs.Add(id.String)
	if isolated {
		b.flush()
	}
}

func (b *bulkSplitter) Get() [][]mongo.WriteModel {
	b.flush()
	return b.bulks
}

func (b *bulkSplitter) currentBulkSize() int {
	return len(b.currentOperations)
}

func (b *bulkSplitter) currentContains(id documentID) bool {
	return b.currentDocumentIDs.Contains(id.String)
}

func (b *bulkSplitter) flush() {
	if b.currentBulkSize() == 0 {
		return
	}
	b.bulks = append(b.bulks, b.currentOperations)
	b.currentOperations = []mongo.WriteModel{}
	b.currentDocumentIDs = *util.NewSet[string]()
}

func newBulkSplitter() bulkSplitter {
	return bulkSplitter{
		bulks:              [][]mongo.WriteModel{},
		currentOperations:  []mongo.WriteModel{},
		currentDocumentIDs: *util.NewSet[string](),
	}
}
