package coordinator

import (
	"fmt"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/errors/coded"
	"github.com/doublecloud/transfer/pkg/terryid"
)

var (
	NoPKey = coded.Register("generic", "no_primary_key")
)

func ReportFakePKey(cp Coordinator, transferID string, category string, fakePkeyTables []abstract.TableID) error {
	if len(fakePkeyTables) == 0 {
		if err := cp.CloseStatusMessagesForCategory(transferID, category); err != nil {
			return xerrors.Errorf("unable to remove warning: %w", err)
		}
		return nil
	}

	if err := cp.OpenStatusMessage(transferID, category, &StatusMessage{
		ID:         terryid.GenerateTransferStatusMessageID(),
		Type:       WarningStatusMessageType,
		Heading:    "Some tables do not have PRIMARY KEYs",
		Message:    fmt.Sprintf("Some tables being transferred do not have PRIMARY KEYs. For these tables, PRIMARY KEY is assumed to consist of all fields of the table. This may negatively affect the throughput of the Transfer. Tables without PRIMARY KEYs: %s", strings.Join(tableFQTNsAsStrings(fakePkeyTables), ", ")),
		Categories: []string{},
		Code:       NoPKey,
	}); err != nil {
		return xerrors.Errorf("unable to add warning: %w", err)
	}
	return nil
}

const FakePKeyStatusMessageCategory string = "fake_primary_key"

func tableFQTNsAsStrings(tIDs []abstract.TableID) []string {
	result := make([]string, len(tIDs))
	for i, tID := range tIDs {
		result[i] = tID.Fqtn()
	}
	return result
}
