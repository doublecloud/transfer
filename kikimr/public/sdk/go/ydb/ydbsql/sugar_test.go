package ydbsql

import (
	"strings"
	"testing"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
)

func TestDeclaration(t *testing.T) {
	for _, test := range []struct {
		decl func(*Declaration)
		exp  string
	}{
		{
			decl: func(d *Declaration) {
				d.Declare("foo", ydb.TypeString)
				d.Declare("bar", ydb.TypeInt64)
				d.Declare("baz", ydb.Struct(
					ydb.StructField("foo", ydb.TypeString),
					ydb.StructField("bar", ydb.TypeInt64),
					ydb.StructField("baz", ydb.Tuple(
						ydb.TypeString, ydb.TypeInt64,
					)),
				))
			},
			exp: strings.Join([]string{
				"DECLARE $foo AS \"String\";",
				"DECLARE $bar AS \"Int64\";",
				"DECLARE $baz AS \"Struct<" +
					"foo:String," +
					"bar:Int64," +
					"baz:Tuple<String,Int64>>\";",
				"",
			}, "\n"),
		},
	} {
		t.Run("", func(t *testing.T) {
			var d Declaration
			test.decl(&d)
			if act, exp := d.String(), test.exp; act != exp {
				t.Fatalf("unexpected declaration: %q; want %q", act, exp)
			}
		})
	}
}
