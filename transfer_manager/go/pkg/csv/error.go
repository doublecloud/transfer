package csv

import "github.com/doublecloud/tross/library/go/core/xerrors"

var (
	errInvalidDelimiter     = xerrors.NewSentinel("csv: invalid delimiter")
	errInvalidEscape        = xerrors.NewSentinel("csv: escape char used outside of quoted text")
	errDoubleQuotesDisabled = xerrors.NewSentinel("csv: found double quotes while double quote feature disabled")
	errQuotingDisabled      = xerrors.NewSentinel("csv: found quote char while feature disabled")
)
