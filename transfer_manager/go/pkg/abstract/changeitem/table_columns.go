package changeitem

type TableColumns []ColSchema

func (s TableColumns) HasPrimaryKey() bool {
	for _, column := range s {
		if column.PrimaryKey {
			return true
		}
	}
	return false
}

func (s TableColumns) HasFakeKeys() bool {
	for _, column := range s {
		if column.FakeKey {
			return true
		}
	}
	return false
}

func (s TableColumns) KeysNum() int {
	result := 0
	for _, column := range s {
		if column.IsKey() {
			result++
		}
	}
	return result
}

func (s TableColumns) ColumnNames() []string {
	result := make([]string, len(s))
	for i, column := range s {
		result[i] = column.ColumnName
	}
	return result
}

func (s TableColumns) Copy() TableColumns {
	result := make(TableColumns, len(s))
	for i := range s {
		result[i] = *(s[i].Copy())
	}
	return result
}
