package types

type ArrayType struct {
	ElementType  DataType
	ContainsNull bool
}

func (a *ArrayType) Name() string {
	return "array"
}
