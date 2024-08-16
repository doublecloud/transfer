package airbyte

import (
	_ "embed"
	"fmt"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/typesystem"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed typesystem.md
	canonDoc string
)

func TestTypeSystem(t *testing.T) {
	rules := typesystem.RuleFor(ProviderType)
	require.NotNil(t, rules.Source)
	require.Nil(t, rules.Target)
	doc := typesystem.Doc(ProviderType, "Airbyte")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}
