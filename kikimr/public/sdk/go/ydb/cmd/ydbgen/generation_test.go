package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func generateFromString(src string) (bytes.Buffer, error) {
	in := bytes.NewReader([]byte(src))
	out := bytes.Buffer{}
	err := generate([]pair{{in, func() io.Writer { return &out }}}, cfg{
		genMode: GenMode{
			Wrap: WrapOptional,
			Seek: SeekColumn,
			Conv: ConvDefault,
		},
		all:     false,
		force:   true,
		verbose: false,
	})
	return out, err
}

func TestGenerator(t *testing.T) {
	for _, test := range testData {
		t.Run(test.in, func(t *testing.T) {
			in := strings.Replace(test.in, "<backtick>", "`", -1)
			out, err := generateFromString(in)
			res := err == nil
			if res != test.correct {
				t.Errorf(
					"test:%q generator(%q) = %t; want %t", test.name,
					in, res, test.correct,
				)
				t.Log(err)
			}
			if err == nil {
				assert.Equal(t, out.String(), test.out)
			}
		})
	}
}
