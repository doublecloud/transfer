package util

func BoolPtr(res bool) *bool {
	return &res
}

func TruePtr() *bool {
	return BoolPtr(true)
}

func FalsePtr() *bool {
	return BoolPtr(false)
}
