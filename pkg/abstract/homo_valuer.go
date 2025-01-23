package abstract

// HomoValuer is the same as sql/driver.Valuer, but for homogenous values.
type HomoValuer interface {
	HomoValue() any
}
