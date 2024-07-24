package util

func Coalesce[Value any](pointer *Value, defaultValue Value) Value {
	if pointer == nil {
		return defaultValue
	}
	return *pointer
}

func CoalesceZero[Value comparable](value Value, defaultValue Value) Value {
	var zero Value
	if value == zero {
		return defaultValue
	}
	return value
}

func CoalesceError(err, defaultErr error) (result error) {
	result = defaultErr
	if err != nil {
		result = err
	}
	return result
}
