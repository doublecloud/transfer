package jsonx

// JSONNull denotes a JSON 'null' value.
// It represents pure 'null' JSON value, not a value inside a JSON object or array. A value inside a JSON object is just golang's 'nil'.
type JSONNull struct{}

func (n JSONNull) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

func (n JSONNull) MarshalYSON() ([]byte, error) {
	return []byte("#"), nil
}

func (n JSONNull) Equals(o any) bool {
	if o == nil {
		return true
	}
	if _, ok := o.(JSONNull); ok {
		return true
	}
	if _, ok := o.(*JSONNull); ok {
		return true
	}
	return false
}
