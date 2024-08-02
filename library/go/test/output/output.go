// Package output provides functions to easily capture stdout/stderr streams data.
//
// Example:
//
//	func TestMyOutput(t *testing.T) {
//	    output.Replace(output.Stdout)
//	    defer output.Reset(output.Stdout)
//
//	    fmt.Println("hello")
//	    captured := output.Catch(output.Stdout)
//	    assert.Equal(t, "hello", string(captured))
//	}
package output

import (
	"bytes"
	"io"
	"os"
)

type Stream int

const (
	Stdout Stream = iota
	Stderr
)

type outputData struct {
	original *os.File
	writer   *os.File
	reader   *os.File
}

var outputs = make(map[Stream]outputData)

// Replace changes required stream with capturable pipe
func Replace(streams ...Stream) error {
	for _, o := range streams {
		r, w, err := os.Pipe()
		if err != nil {
			return err
		}

		var data outputData
		switch o {
		case Stdout:
			data.original = os.Stdout
			os.Stdout = w
		case Stderr:
			data.original = os.Stderr
			os.Stderr = w
		}

		data.writer = w
		data.reader = r

		outputs[o] = data
	}

	return nil
}

// Catch receives and returns stream output
func Catch(o Stream) []byte {
	outC := make(chan []byte)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, outputs[o].reader)
		outC <- buf.Bytes()
	}()

	_ = outputs[o].writer.Close()

	out := <-outC
	return out
}

// Reset returns original stream back to its place
func Reset(o Stream) {
	switch o {
	case Stdout:
		os.Stdout = outputs[o].original
	case Stderr:
		os.Stderr = outputs[o].original
	}
	_ = outputs[o].reader.Close()
	delete(outputs, o)
}

// ResetAll returns all original streams back to their places
func ResetAll() {
	for o := range outputs {
		Reset(o)
	}
}
