package table

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestResultAny(t *testing.T) {
	for _, test := range []struct {
		name    string
		columns []Column
		values  []ydb.Value
		exp     []interface{}
	}{
		{
			columns: []Column{
				{"column0", ydb.Optional(ydb.TypeUint32), "family0"},
			},
			values: []ydb.Value{
				ydb.OptionalValue(ydb.Uint32Value(43)),
				ydb.NullValue(ydb.TypeUint32),
			},
			exp: []interface{}{
				uint32(43),
				nil,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := NewResult(
				NewResultSet(
					WithColumns(test.columns...),
					WithValues(test.values...),
				),
			)
			var i int
			var act interface{}
			for res.NextSet() {
				for res.NextRow() {
					err := res.ScanWithDefaults(&act)
					if err != nil {
						t.Fatal(err)
					}
					if exp := test.exp[i]; !reflect.DeepEqual(act, exp) {
						t.Errorf(
							"unexpected Any() result: %[1]v (%[1]T); want %[2]v (%[2]T)",
							act, exp,
						)
					}
					i++
				}
			}
			if err := res.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResultOUint32(t *testing.T) {
	for _, test := range []struct {
		name    string
		columns []Column
		values  []ydb.Value
		exp     []uint32
	}{
		{
			columns: []Column{
				{"column0", ydb.Optional(ydb.TypeUint32), "family0"},
				{"column1", ydb.TypeUint32, "family0"},
			},
			values: []ydb.Value{
				ydb.OptionalValue(ydb.Uint32Value(43)),
				ydb.Uint32Value(43),
			},
			exp: []uint32{
				43,
				43,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := NewResult(
				NewResultSet(
					WithColumns(test.columns...),
					WithValues(test.values...),
				),
			)
			var i int
			for res.NextSet() {
				for res.NextRow() {
					res.NextItem()
					act := res.OUint32()
					if exp := test.exp[i]; !reflect.DeepEqual(act, exp) {
						t.Errorf(
							"unexpected OUint32() result: %[1]v (%[1]T); want %[2]v (%[2]T)",
							act, exp,
						)
					}
					i++
				}
			}
			if err := res.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type resultSetDesc Ydb.ResultSet

type ResultSetOption func(*resultSetDesc)

func WithColumns(cs ...Column) ResultSetOption {
	return func(r *resultSetDesc) {
		for _, c := range cs {
			r.Columns = append(r.Columns, &Ydb.Column{
				Name: c.Name,
				Type: internal.TypeToYDB(c.Type),
			})
		}
	}
}

func WithValues(vs ...ydb.Value) ResultSetOption {
	return func(r *resultSetDesc) {
		n := len(r.Columns)
		if n == 0 {
			panic("empty columns")
		}
		if len(vs)%n != 0 {
			panic("malformed values set")
		}
		var row *Ydb.Value
		for i, v := range vs {
			j := i % n
			if j == 0 && i > 0 {
				r.Rows = append(r.Rows, row)
			}
			if j == 0 {
				row = &Ydb.Value{
					Items: make([]*Ydb.Value, n),
				}
			}
			tv := internal.ValueToYDB(v)
			act := internal.TypeFromYDB(tv.Type)
			exp := internal.TypeFromYDB(r.Columns[j].Type)
			if !internal.TypesEqual(act, exp) {
				panic(fmt.Sprintf(
					"unexpected type for #%d column: %s; want %s",
					j, act, exp,
				))
			}
			row.Items[j] = tv.Value
		}
		if row != nil {
			r.Rows = append(r.Rows, row)
		}
	}
}

func NewResultSet(opts ...ResultSetOption) *Ydb.ResultSet {
	var d resultSetDesc
	for _, opt := range opts {
		opt(&d)
	}
	return (*Ydb.ResultSet)(&d)
}

func NewResult(sets ...*Ydb.ResultSet) *Result {
	return &Result{
		sets: sets,
	}
}
