package abstract

type TransferType string

const (
	TransferTypeNone                 = TransferType("TRANSFER_TYPE_UNSPECIFIED")
	TransferTypeSnapshotAndIncrement = TransferType("SNAPSHOT_AND_INCREMENT")
	TransferTypeSnapshotOnly         = TransferType("SNAPSHOT_ONLY")
	TransferTypeIncrementOnly        = TransferType("INCREMENT_ONLY")
)

func (t *TransferType) Expand() []TransferType {
	switch *t {
	case TransferTypeSnapshotAndIncrement:
		return []TransferType{TransferTypeSnapshotAndIncrement, TransferTypeSnapshotOnly, TransferTypeIncrementOnly}
	case TransferTypeSnapshotOnly:
		return []TransferType{TransferTypeSnapshotOnly}
	case TransferTypeIncrementOnly:
		return []TransferType{TransferTypeIncrementOnly}
	}
	return []TransferType{}
}
