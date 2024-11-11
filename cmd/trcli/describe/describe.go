package describe

import (
	"fmt"

	"github.com/charmbracelet/glamour"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/cobraaux"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func DescribeCommand() *cobra.Command {
	var sourceType string
	source := &cobra.Command{
		Use:   "source",
		Short: "Describe transfer pair",
		Args:  cobra.MatchAll(cobra.ExactArgs(0)),
		RunE: generate(func() (model.EndpointParams, error) {
			return model.NewSource(abstract.ProviderType(sourceType), "{}")
		}),
	}
	source.Flags().StringVar(&sourceType, "type", "pg", fmt.Sprintf("Type of source to generate docs, one of: %v", model.KnownSources()))
	var targetType string
	destination := &cobra.Command{
		Use:   "destination",
		Short: "Describe transfer pair",
		Args:  cobra.MatchAll(cobra.ExactArgs(0)),
		RunE: generate(func() (model.EndpointParams, error) {
			return model.NewDestination(abstract.ProviderType(targetType), "{}")
		}),
	}
	destination.Flags().StringVar(&targetType, "type", "ch", fmt.Sprintf("Type of destination to generate docs, one of: %v", model.KnownDestinations()))

	describe := &cobra.Command{
		Use:   "describe",
		Short: "Describe endpoint type",
	}
	cobraaux.RegisterCommand(describe, source)
	cobraaux.RegisterCommand(describe, destination)
	return describe
}

func generate(f func() (model.EndpointParams, error)) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		endpoint, err := f()
		if err != nil {
			return xerrors.Errorf("unable to build endpoint: %w", err)
		}
		return describeEndpoint(endpoint)
	}
}

func describeEndpoint(e model.EndpointParams) error {
	if e, ok := e.(model.Describable); ok {
		out, _ := glamour.RenderWithEnvironmentConfig(fmt.Sprintf(`
%s

# Example Config
%s
`, e.Describe().Usage, "```yaml\n"+e.Describe().Example+"```"))
		fmt.Print(out)
		return nil
	}
	e.WithDefaults()
	data, err := yaml.Marshal(e)
	if err != nil {
		return xerrors.Errorf("unable to marshal endpoint params: %w", err)
	}
	out, _ := glamour.RenderWithEnvironmentConfig(fmt.Sprintf(`
# Usage: %s

# Default Config
%s
`, e.GetProviderType().Name(), "```yaml\n"+string(data)+"```"))
	fmt.Print(out)
	return nil
}
