package helpers

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/dataplane/provideradapter"
	"golang.org/x/exp/slices"
)

var TransferID = "dtt"

func EmptyRegistry() metrics.Registry {
	return solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})
}

func GetEnvOfFail(t *testing.T, key string) string {
	res, ok := os.LookupEnv(key)
	if !ok {
		t.Fail()
	}
	return res
}

func GetIntFromEnv(varName string) int {
	val, err := strconv.Atoi(os.Getenv(varName))
	if err != nil {
		panic(err)
	}
	return val
}

// StrictEquality - default callback for checksum - just compare typeNames
func StrictEquality(l, r string) bool {
	return l == r
}

func InitSrcDst(transferID string, src server.Source, dst server.Destination, transferType abstract.TransferType) {
	src.WithDefaults()
	dst.WithDefaults()

	transfer := &server.Transfer{
		ID:   transferID,
		Type: transferType,
		Src:  src,
		Dst:  dst,
	}
	// fill dependent fields on drugs
	_ = provideradapter.ApplyForTransfer(transfer)
	transfer.FillDependentFields()
}

func MakeTransfer(transferID string, src server.Source, dst server.Destination, transferType abstract.TransferType) *server.Transfer {
	src.WithDefaults()
	dst.WithDefaults()
	transfer := &server.Transfer{
		ID:   transferID,
		Type: transferType,
		Src:  src,
		Dst:  dst,
	}
	transfer.FillDependentFields()
	// fill dependent fields on drugs
	_ = provideradapter.ApplyForTransfer(transfer)

	return transfer
}

func WithLocalRuntime(transfer *server.Transfer, jobCount int, processCount int) *server.Transfer {
	transfer.Runtime = &abstract.LocalRuntime{
		Host:       "",
		CurrentJob: 0,
		ShardingUpload: abstract.ShardUploadParams{
			JobCount:     jobCount,
			ProcessCount: processCount,
		},
	}
	return transfer
}

func MakeTransferForIncrementalSnapshot(transferID string, src server.Source, dst server.Destination, transferType abstract.TransferType,
	namespace, tableName, cursorField, initialState string, incrementDelay int64) *server.Transfer {

	regularSnapshot := &abstract.RegularSnapshot{
		Incremental: []abstract.IncrementalTable{
			{Namespace: namespace, Name: tableName, CursorField: cursorField, InitialState: initialState},
		},
		IncrementDelaySeconds: incrementDelay,
		CronExpression:        "",
	}

	transfer := &server.Transfer{
		ID:              transferID,
		Type:            transferType,
		Src:             src,
		Dst:             dst,
		RegularSnapshot: regularSnapshot,
	}
	transfer.FillDependentFields()
	return transfer
}

// GetPortFromStr - works when the port is in the end of the string, preceded by a colon
func GetPortFromStr(s string) (int, error) {
	tokens := strings.Split(s, ":")
	if tokens[0] == s {
		return 1, xerrors.Errorf("Unable to find port in string %v (no colon)", s)
	}
	portStr := tokens[len(tokens)-1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 1, xerrors.Errorf("Unable to get port from string %v (unable to parse %v)", s, portStr)
	}
	return port, nil
}

// RemoveColumnsFromChangeItem removes ColumnNames[i] and ColumnValues[i] where ColumnNames[i] is in columnsToRemove.
func RemoveColumnsFromChangeItem(item abstract.ChangeItem, columnsToRemove []string) abstract.ChangeItem {
	res := item
	res.ColumnNames = nil
	res.ColumnValues = nil
	for i, colName := range item.ColumnNames {
		if !slices.Contains(columnsToRemove, colName) {
			res.ColumnNames = append(res.ColumnNames, colName)
			res.ColumnValues = append(res.ColumnValues, item.ColumnValues[i])
		}
	}
	return res
}
