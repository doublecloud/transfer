package tasks

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
)

//---------------------------------------------------------------------------------------------------------------------
// This file is needed for lb-in-the-middle cases
//
// 'terminal' here - is NOT(lb) endpoint
// terminal_transfer - transfer where at least one endpoint is terminal
//
// We support here either terminal->terminal or one-lb-in-the-middle cases
// If there are more than 1 lb in chain - we are not supporting this.
// It's not fundamental restriction, we can support long chains, but we need good reasons for that!
//
// terminal transfers:
//     - terminal->lb       - left terminal type
//     - lb->terminal       - right terminal type
//     - terminal->terminal
//---------------------------------------------------------------------------------------------------------------------

func GetLeftTerminalTransfers(cp coordinator.Coordinator, transfer server.Transfer) ([]*server.Transfer, error) {
	if isLeftTerminalType(transfer) {
		return []*server.Transfer{&transfer}, nil
	} else {
		return getLeftTerminalTransfersForLbSrc(cp, transfer)
	}
}

func GetLeftTerminalSrcEndpoints(cp coordinator.Coordinator, transfer server.Transfer) ([]server.Source, error) {
	transfers, err := GetLeftTerminalTransfers(cp, transfer)
	if err != nil {
		return nil, err
	}
	result := make([]server.Source, 0)
	for _, currTransfer := range transfers {
		result = append(result, currTransfer.Src)
	}
	return result, nil
}

func GetRightTerminalTransfers(cp coordinator.Coordinator, transfer server.Transfer) ([]*server.Transfer, error) {
	if isRightTerminalType(transfer) {
		return []*server.Transfer{&transfer}, nil
	} else {
		return getRightTerminalTransfersForLbDst(cp, transfer)
	}
}

func GetRightTerminalDstEndpoints(cp coordinator.Coordinator, transfer server.Transfer) ([]server.Destination, error) {
	transfers, err := GetRightTerminalTransfers(cp, transfer)
	if err != nil {
		return nil, err
	}
	result := make([]server.Destination, 0)
	for _, currTransfer := range transfers {
		result = append(result, currTransfer.Dst)
	}
	return result, nil
}

func isLeftTerminalType(transfer server.Transfer) bool {
	switch transfer.Src.(type) {
	case server.TransitionalEndpoint:
		return false
	default:
		return true
	}
}

func isRightTerminalType(transfer server.Transfer) bool {
	switch transfer.Dst.(type) {
	case server.TransitionalEndpoint:
		return false
	default:
		return true
	}
}

func getLeftTerminalTransfersForLbSrc(cp coordinator.Coordinator, transfer server.Transfer) ([]*server.Transfer, error) {
	result := make([]*server.Transfer, 0)

	transfers, err := cp.GetTransfers([]server.TransferStatus{server.Running}, transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfers: %w", err)
	}

	src, ok := transfer.Src.(server.TransitionalEndpoint)

	if !ok {
		return nil, xerrors.Errorf("transfer %v is not lb-src", transfer)
	}

	for _, currTransfer := range transfers {
		if dst, ok := currTransfer.Dst.(server.TransitionalEndpoint); ok {
			if dst.TransitionalWith(src) {
				logger.Log.Infof("Found lb origin %v", currTransfer.ID)
				if isLeftTerminalType(*currTransfer) {
					result = append(result, currTransfer)
				}
			}
		}
	}

	return result, nil
}

func getRightTerminalTransfersForLbDst(cp coordinator.Coordinator, transfer server.Transfer) ([]*server.Transfer, error) {
	result := make([]*server.Transfer, 0)

	transfers, err := cp.GetTransfers([]server.TransferStatus{server.Running}, transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfers: %w", err)
	}

	src, ok := transfer.Dst.(server.TransitionalEndpoint)

	if !ok {
		return nil, xerrors.Errorf("transfer %v is not lb-dst", transfer)
	}

	for _, currTransfer := range transfers {
		if dst, ok := currTransfer.Src.(server.TransitionalEndpoint); ok {
			if dst.TransitionalWith(src) {
				logger.Log.Infof("Found lb target %v", currTransfer.ID)
				if isRightTerminalType(*currTransfer) {
					result = append(result, currTransfer)
				}
			}
		}
	}

	return result, nil
}
