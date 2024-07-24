package changeitem

type EventSize struct {
	// Read is the byte length of data read to produce an item
	Read uint64
	// Values is the size of ColumnValues
	Values uint64
}

func RawEventSize(readBytes uint64) EventSize {
	return EventSize{
		Read:   readBytes,
		Values: 0,
	}
}

func EmptyEventSize() EventSize {
	return EventSize{
		Read:   0,
		Values: 0,
	}
}
