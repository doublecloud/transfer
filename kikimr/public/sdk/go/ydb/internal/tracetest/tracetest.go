package tracetest

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/traceutil"
)

// TestCompose is a generic function for testing trace composing.
// It calls given compose function with two instances of type of x and inspects
// result.
//
// In a nutshell, it uses reflection. It creates two instances of x's type, say
// x' and x", fills each functional field of instances with stub function and
// then calls compose(x', x"). It then calls each functional field of composed
// struct – if some fields are not called in x' or x" then it fails the test.
func TestCompose(t *testing.T, compose, x interface{}) {
	a := reflect.New(reflect.TypeOf(x))
	b := reflect.New(reflect.TypeOf(x))

	defer assertCalled(t, "first:", stubEachFunc(a))
	defer assertCalled(t, "second:", stubEachFunc(b))

	composeFn := reflect.ValueOf(compose)
	ct := composeFn.Type()
	if ct.Kind() != reflect.Func {
		t.Fatal("given compose is not a function")
	}
	if ct.NumIn() != 2 || ct.NumOut() != 1 {
		t.Fatal("unexpected compose function signature")
	}
	args := []reflect.Value{
		a.Elem(),
		b.Elem(),
	}
	for i, arg := range args {
		if act, exp := arg.Type(), ct.In(i); act != exp {
			t.Fatalf(
				"unexpected type of %d struct: %s; composer want %s",
				i, act, exp,
			)
		}
	}

	r := composeFn.Call(args)
	callEachFunc(r[0])
}

func TestSingleTrace(t *testing.T, x interface{}, traceName string) {
	a := reflect.New(reflect.TypeOf(x))
	defer assertCalled(t, traceName, stubEachFunc(a))
	callEachFunc(a.Elem())
}

func assertCalled(t *testing.T, prefix string, called map[string]bool) {
	for name, called := range called {
		if !called {
			t.Error(prefix, fmt.Sprintf("%s field is not called", name))
		}
	}
}

func stubEachFunc(x reflect.Value) map[string]bool {
	fs := make(map[string]bool)
	(traceutil.FieldStubber{
		OnStub: func(name string) {
			fs[name] = false
		},
		OnCall: func(name string, _ ...interface{}) {
			fs[name] = true
		},
	}).Stub(x)
	return fs
}

func callEachFunc(x reflect.Value) {
	var (
		t = x.Type()
	)
	for i := 0; i < t.NumField(); i++ {
		var (
			f  = x.Field(i)
			ft = f.Type()
		)
		if ft.Kind() != reflect.Func {
			continue
		}
		if f.IsNil() {
			continue
		}
		args := make([]reflect.Value, ft.NumIn())
		for i := range args {
			args[i] = reflect.New(ft.In(i)).Elem()
		}
		f.Call(args)
	}
}
