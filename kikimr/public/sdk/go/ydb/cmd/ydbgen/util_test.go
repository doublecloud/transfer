package main

import "testing"

func TestCamelToSnake(t *testing.T) {
	for _, test := range []struct {
		in  string
		exp string
	}{
		{
			in:  "Name",
			exp: "name",
		},
		{
			in:  "CamelCase",
			exp: "camel_case",
		},
		{
			in:  "HTTPRequest",
			exp: "http_request",
		},
		{
			in:  "SomeHTTPRequest",
			exp: "some_http_request",
		},
		{
			in:  "ID",
			exp: "id",
		},
		{
			in:  "HTTP",
			exp: "http",
		},
		{
			in:  "aBa",
			exp: "a_ba",
		},
		{
			in:  "aBaB",
			exp: "a_ba_b",
		},
	} {
		t.Run(test.in, func(t *testing.T) {
			act := camelToSnake(test.in)
			if exp := test.exp; act != exp {
				t.Errorf(
					"camelToSnake(%q) = %q; want %q",
					test.in, act, exp,
				)
			}
		})
	}
}
