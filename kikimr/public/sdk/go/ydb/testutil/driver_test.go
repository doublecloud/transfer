package testutil

import "testing"

func TestGetField(t *testing.T) {
	type Bar struct {
		Baz string
	}
	type Foo struct {
		Bar *Bar
	}
	for _, test := range []struct {
		name string
		path string
		src  interface{}

		exp string
	}{
		{
			src: &Foo{
				Bar: &Bar{
					Baz: "value",
				},
			},
			exp:  "value",
			path: "Bar.Baz",
		},
		{
			src: &Bar{
				Baz: "value",
			},
			exp:  "value",
			path: "Baz",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var act string
			getField(test.path, test.src, &act)
			if exp := test.exp; act != exp {
				t.Fatalf("getField(%q) = %q; want %q", test.path, act, exp)
			}
		})
	}
}
