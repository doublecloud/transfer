package parser

import (
	"embed"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/internal/metrics"
	"github.com/doublecloud/transfer/pkg/abstract"
	parsersfactory "github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/doublecloud/transfer/tests/canon/parser/testcase"
	"github.com/doublecloud/transfer/tests/canon/validator"
	"github.com/stretchr/testify/require"
)

//go:embed samples/static/generic/*
var TestGenericSamples embed.FS

func TestGenericParsers(t *testing.T) {
	cases := testcase.LoadStaticTestCases(t, TestGenericSamples)

	for tc := range cases {
		t.Run(tc, func(t *testing.T) {
			currCase := cases[tc]
			parser, err := parsersfactory.NewParserFromParserConfig(currCase.ParserConfig, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{
				"id": "TestParser_Do",
			})))
			require.NoError(t, err)
			require.NotNil(t, parser)
			res := parser.Do(currCase.Data, abstract.Partition{Topic: currCase.TopicName})
			require.NotNil(t, res)
			sink := validator.New(
				false,
				validator.ValuesTypeChecker,
				validator.Canonizator(t),
			)()
			require.NoError(t, sink.Push(res))
			require.NoError(t, sink.Close())
		})
	}
}
