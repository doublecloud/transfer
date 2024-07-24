package traceutil

import (
	"reflect"
)

// Stub is a helper function that stubs all functional fields of x with given
// f.
func Stub(x interface{}, f func(name string, args ...interface{})) {
	(FieldStubber{
		OnCall: f,
	}).Stub(reflect.ValueOf(x))
}

func ClearContext(x interface{}) interface{} {
	p := reflect.ValueOf(x).Index(0)
	t := p.Elem().Type()
	f, has := t.FieldByName("Context")
	if has && f.Type.Kind() == reflect.Interface {
		x := reflect.New(t)
		x.Elem().Set(p.Elem())
		c := x.Elem().FieldByName(f.Name)
		c.Set(reflect.Zero(c.Type()))
		p.Set(x)
	}
	return p.Interface()
}

// FieldStubber contains options of filling all struct functional fields.
type FieldStubber struct {
	// OnStub is an optional callback that is called when field getting
	// stubbed.
	OnStub func(name string)

	// OnCall is an optional callback that will be called for each stubbed
	// field getting called.
	OnCall func(name string, args ...interface{})
}

// Stub fills in given x struct.
func (f FieldStubber) Stub(x reflect.Value) {
	var (
		v = x.Elem()
		t = v.Type()
	)
	for i := 0; i < t.NumField(); i++ {
		var (
			fx = v.Field(i)
			ft = fx.Type()
		)
		if ft.Kind() != reflect.Func {
			continue
		}
		name := t.Field(i).Name
		if f.OnStub != nil {
			f.OnStub(name)
		}
		out := []reflect.Value{}
		for i := 0; i < ft.NumOut(); i++ {
			ti := reflect.New(ft.Out(i)).Elem()
			out = append(out, ti)
		}
		fn := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
			if f.OnCall == nil {
				return out
			}
			params := make([]interface{}, len(args))
			for i, arg := range args {
				params[i] = arg.Interface()
			}
			f.OnCall(name, params...)
			return out
		})
		fx.Set(fn)
	}
}
