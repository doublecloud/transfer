package abstract

type Block interface {
	GetData() []map[string]interface{}
}

type ColumnType interface {
	GetRepresentation() string
}

type Progress struct {
	Completed int64
	Total     int64
}

type ColumnInfo struct {
	Name     string
	Type     *ColumnType
	Nullable bool
}

type DateColumn struct {
	From, To, Nano string
}
