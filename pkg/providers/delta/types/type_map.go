package types

type MapType struct {
	KeyType           DataType
	ValueType         DataType
	ValueContainsNull bool
}

func (m *MapType) Name() string {
	return "map"
}
