package util

import (
	"bytes"
	"encoding/json"
)

// JSONMarshalUnescape writes the JSON encoding of v to the byte slice,
// does not make HTML Escape (like json.Marshal).
func JSONMarshalUnescape(v any) ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	// delete the newline character at the end of the encoded json
	return buf.Bytes()[0 : len(buf.Bytes())-1], nil
}
