package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"text/template"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

var query = template.Must(template.New("fill database").Parse(`
	DECLARE $var AS Variant<Utf8,Uint64,Uint32>;

	$struct = AsStruct(
		Uint32("0") as foo,
		UTF8("x") as bar,
		Int64("0") as baz,
	);
	$variantStructType = VariantType(TypeOf($struct));
	SELECT Variant(42, "baz", $variantStructType);

	SELECT
		AsList("foo", "bar", "baz");
	SELECT
		AsTuple(42, "foo", AsList(41, 42, 43));
	SELECT
		AsDict(
			AsTuple("foo", 10),
			AsTuple("bar", 20),
			AsTuple("baz", 30),
		);
	SELECT
		AsStruct(
			41 AS foo,
			42 AS bar,
			43 AS baz,
		);

	$tuple = AsTuple(
		Uint32("0"),
		UTF8("x"),
		Int64("0"),
	);
	$variantTupleType = VariantType(TypeOf($tuple));
	SELECT Variant(42, "2", $variantTupleType);
`))

type Command struct {
}

type exampleStruct struct {
}

func (*exampleStruct) UnmarshalYDB(res ydb.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.StructIn(); i < n; i++ {
		name := res.StructField(i)
		if res.IsNull() {
			continue
		}
		val := res.Int32()
		log.Printf("(struct): %s: %d", name, val)
	}
	res.StructOut()
	return nil
}

type exampleList struct {
}

func (*exampleList) UnmarshalYDB(res ydb.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.ListIn(); i < n; i++ {
		res.ListItem(i)
		log.Printf("(list) %q: %s", res.Path(), res.String())
	}
	res.ListOut()
	return nil
}

type exampleTuple struct {
}

func (*exampleTuple) UnmarshalYDB(res ydb.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.TupleIn(); i < n; i++ {
		res.TupleItem(i)
		switch i {
		case 0:
			log.Printf("(tuple) %q: %d", res.Path(), res.Int32())
		case 1:
			log.Printf("(tuple) %q: %s", res.Path(), res.String())
		case 2:
			n := res.ListIn()
			for j := 0; j < n; j++ {
				res.ListItem(j)
				log.Printf("(tuple) %q: %d", res.Path(), res.Int32())
			}
			res.ListOut()
		}
	}
	res.TupleOut()
	return nil
}

type exampleDict struct {
}

func (*exampleDict) UnmarshalYDB(res ydb.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.DictIn(); i < n; i++ {
		res.DictKey(i)
		key := res.String()

		res.DictPayload(i)
		val := res.Int32()

		log.Printf("(dict) %q: %s: %d", res.Path(), key, val)
	}
	res.DictOut()
	return nil
}

type variantStruct struct {
}

func (*variantStruct) UnmarshalYDB(res ydb.RawValue) error {
	log.Printf("T: %s", res.Type())
	name, index := res.Variant()
	var x interface{}
	switch name {
	case "foo":
		x = res.Uint32()
	case "bar":
		x = res.UTF8()
	case "baz":
		x = res.Int64()
	}
	log.Printf(
		"(struct variant): %s %s %q %d = %v",
		res.Path(), res.Type(), name, index, x,
	)
	return nil
}

type variantTuple struct {
}

func (*variantTuple) UnmarshalYDB(res ydb.RawValue) error {
	log.Printf("T: %s", res.Type())
	name, index := res.Variant()
	var x interface{}
	switch index {
	case 0:
		x = res.Uint32()
	case 1:
		x = res.UTF8()
	case 2:
		x = res.Int64()
	}
	log.Printf(
		"(tuple variant): %s %s %q %d = %v",
		res.Path(), res.Type(), name, index, x,
	)
	return nil
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	session, err := db.Table().CreateSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = session.Close(context.Background())
	}()

	tx, err := session.BeginTransaction(ctx, table.TxSettings(
		table.WithSerializableReadWrite(),
	))
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	res, err := tx.Execute(ctx, render(query, nil), nil)
	if err != nil {
		return err
	}
	if _, err = tx.CommitTx(ctx); err != nil {
		return err
	}

	parsers := [...]func(){
		func() {
			_ = res.Scan(&variantStruct{})
		},
		func() {
			_ = res.Scan(&exampleList{})
		},
		func() {
			_ = res.Scan(&exampleTuple{})
		},
		func() {
			_ = res.Scan(&exampleDict{})
		},
		func() {
			_ = res.Scan(&exampleStruct{})
		},
		func() {
			_ = res.Scan(&variantTuple{})
		},
	}
	for set := 0; res.NextSet(); set++ {
		res.NextRow()
		parsers[set]()
		if err = res.Err(); err != nil {
			return err
		}
	}
	if err = res.Err(); err != nil {
		return err
	}

	return nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
