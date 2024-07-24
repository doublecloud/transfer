package common

import (
	"fmt"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
)

type TransactionInfo struct {
	id    string
	begin *LogPosition
	end   *LogPosition
}

func NewTransactionInfo(id string, begin *LogPosition, end *LogPosition) *TransactionInfo {
	return &TransactionInfo{
		id:    id,
		begin: begin,
		end:   end,
	}
}

func (info *TransactionInfo) ID() string {
	return info.id
}

func (info *TransactionInfo) OracleBeginPosition() *LogPosition {
	return info.begin
}

func (info *TransactionInfo) OracleEndPosition() *LogPosition {
	return info.end
}

// base.Transaction

func (info *TransactionInfo) BeginPosition() base.LogPosition {
	return info.begin
}

func (info *TransactionInfo) EndPosition() base.LogPosition {
	return info.end
}

func (info *TransactionInfo) Equals(otherTransaction base.Transaction) bool {
	if otherOracleTransaction, ok := otherTransaction.(*TransactionInfo); !ok {
		return false
	} else {
		return info.ID() == otherOracleTransaction.ID()
	}
}

func (info *TransactionInfo) String() string {
	return fmt.Sprintf("%v: From [%v], To [%v]", info.id, info.begin, info.end)
}
