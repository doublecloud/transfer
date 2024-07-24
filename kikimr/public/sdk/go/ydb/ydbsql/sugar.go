package ydbsql

import (
	"bytes"
	"io"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
)

type Declaration struct {
	buf bytes.Buffer
}

func (d *Declaration) WriteTo(dest io.Writer) (int64, error) {
	return d.buf.WriteTo(dest)
}

func (d *Declaration) String() string {
	return d.buf.String()
}

func (d *Declaration) Declare(name string, t ydb.Type) {
	d.buf.WriteString("DECLARE $")
	d.buf.WriteString(name)
	d.buf.WriteString(" AS \"")
	internal.WriteTypeStringTo(&d.buf, t)
	d.buf.WriteString("\";\n")
}
