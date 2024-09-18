package example

import (
	"testing"

	"github.com/doublecloud/transfer/recipe/mongo/pkg/util"
)

func TestSample(t *testing.T) {
	util.TestMongoShardedClusterRecipe(t)
}
