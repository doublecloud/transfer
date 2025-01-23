package sink

type ObjectRange struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

func NewObjectRange(from, to uint64) ObjectRange {
	return ObjectRange{
		From: from,
		To:   to,
	}
}
