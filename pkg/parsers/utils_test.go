package parsers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetParserConfigNameByStruct(t *testing.T) {
	type ParserConfigBestFormatLb struct{}
	require.Equal(t, "best_format.lb", ParserConfigNameByStruct(ParserConfigBestFormatLb{}))
	type ParserConfigBestFormatCommon struct{}
	require.Equal(t, "best_format.common", ParserConfigNameByStruct(ParserConfigBestFormatCommon{}))
}

func TestGetParserNameByStruct(t *testing.T) {
	type ParserConfigBestFormatLb struct{}
	require.Equal(t, "best_format", getParserNameByStruct(ParserConfigBestFormatLb{}))
	type ParserConfigBestFormatCommon struct{}
	require.Equal(t, "best_format", getParserNameByStruct(ParserConfigBestFormatCommon{}))
}

func TestIsThisParserConfig(t *testing.T) {
	type ParserConfigBestFormatLb struct{}
	type ParserConfigBestFormatCommon struct{}
	require.True(t, IsThisParserConfig(map[string]interface{}{"best_format.common": nil}, ParserConfigBestFormatCommon{}))
	require.False(t, IsThisParserConfig(map[string]interface{}{"best_format.common": nil}, ParserConfigBestFormatLb{}))
	require.True(t, IsThisParserConfig(map[string]interface{}{"best_format.common": nil}, &ParserConfigBestFormatCommon{}))
	require.False(t, IsThisParserConfig(map[string]interface{}{"best_format.common": nil}, &ParserConfigBestFormatLb{}))
}
