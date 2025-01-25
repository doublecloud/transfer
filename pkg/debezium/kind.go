package debezium

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

func kindToOp(kind abstract.Kind, snapshot bool, emitType emitType) (string, error) {
	switch kind {
	case abstract.InsertKind:
		if snapshot {
			return "r", nil
		} else {
			return "c", nil
		}
	case abstract.UpdateKind:
		switch emitType {
		case regularEmitType:
			return "u", nil
		case deleteEventEmitType:
			return "d", nil
		case insertEventEmitType:
			return "c", nil
		default:
			return "", xerrors.Errorf("unsupported emitType: %d", emitType)
		}
	case abstract.DeleteKind:
		return "d", nil
	default:
		return "", xerrors.Errorf("unsupported kind: %s", kind)
	}
}

func opToKind(op string) (abstract.Kind, error) {
	switch op {
	case "c":
		return abstract.InsertKind, nil
	case "r":
		return abstract.InsertKind, nil
	case "u":
		return abstract.UpdateKind, nil
	case "d":
		return abstract.DeleteKind, nil
	default:
		return abstract.InsertKind, xerrors.Errorf("unknown op: %s", op)
	}
}
