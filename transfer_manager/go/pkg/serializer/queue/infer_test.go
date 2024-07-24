package queue

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func normalize(in string) string {
	pos := strings.Index(in, "func InferFormatSettings")
	result := in[pos:]
	result = strings.ReplaceAll(result, "logbroker.LfSource", "LfSource")
	result = strings.ReplaceAll(result, "logbroker.ProviderType", "ProviderType")
	return result
}

func TestInferFuncSameInAllSources(t *testing.T) {
	t.SkipNow()

	paths := []string{
		"../../providers/kafka/provider_utils.go",
		"../../providers/logbroker/provider_utils.go",
		"../../providers/yds/provider_utils.go",
	}

	files := make([]string, 0)

	for _, path := range paths {
		buf, err := os.ReadFile(path)
		require.NoError(t, err)
		files = append(files, string(buf))
	}

	for i := 1; i < len(files); i++ {
		l := normalize(files[0])
		r := normalize(files[i])
		require.Equal(t, l, r)
	}
}
