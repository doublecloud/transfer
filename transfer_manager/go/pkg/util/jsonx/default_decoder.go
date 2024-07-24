package jsonx

import (
	"bytes"
	"encoding/json"
	"io"
)

// NewDefaultDecoder constructs a default JSON decoder for Data Transfer.
func NewDefaultDecoder(r io.Reader) *json.Decoder {
	result := json.NewDecoder(r)
	result.UseNumber()
	return result
}

// Unmarshal decodes body by a default JSON decoder for Data Transfer.
func Unmarshal(body []byte, result any) error {
	return NewDefaultDecoder(bytes.NewReader(body)).Decode(&result)
}
