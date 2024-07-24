/*
Package opt contains go basic types wrappers to be used when dealing with
optional types. The main intention of this package is to provide support for
code generation with optional types.
*/
package opt

type Bool struct {
	value   bool
	defined bool
}

func OBool(v bool) (r Bool) {
	r.Set(v)
	return
}
func (b Bool) Get() (v bool, ok bool) {
	if b.defined {
		return b.value, true
	}
	return
}
func (b *Bool) Set(v bool) {
	*b = Bool{
		value:   v,
		defined: true,
	}
}

type Int struct {
	value   int
	defined bool
}

func OInt(v int) (r Int) {
	r.Set(v)
	return
}
func (i Int) Get() (v int, ok bool) {
	if i.defined {
		return i.value, true
	}
	return
}
func (i *Int) Set(v int) {
	*i = Int{
		value:   v,
		defined: true,
	}
}

type Int8 struct {
	value   int8
	defined bool
}

func OInt8(v int8) (r Int8) {
	r.Set(v)
	return
}
func (i Int8) Get() (v int8, ok bool) {
	if i.defined {
		return i.value, true
	}
	return
}
func (i *Int8) Set(v int8) {
	*i = Int8{
		value:   v,
		defined: true,
	}
}

type Int16 struct {
	value   int16
	defined bool
}

func OInt16(v int16) (r Int16) {
	r.Set(v)
	return
}
func (i Int16) Get() (v int16, ok bool) {
	if i.defined {
		return i.value, true
	}
	return
}
func (i *Int16) Set(v int16) {
	*i = Int16{
		value:   v,
		defined: true,
	}
}

type Int32 struct {
	value   int32
	defined bool
}

func OInt32(v int32) (r Int32) {
	r.Set(v)
	return
}
func (i Int32) Get() (v int32, ok bool) {
	if i.defined {
		return i.value, true
	}
	return
}
func (i *Int32) Set(v int32) {
	*i = Int32{
		value:   v,
		defined: true,
	}
}

type Int64 struct {
	value   int64
	defined bool
}

func OInt64(v int64) (r Int64) {
	r.Set(v)
	return
}
func (i Int64) Get() (v int64, ok bool) {
	if i.defined {
		return i.value, true
	}
	return
}
func (i *Int64) Set(v int64) {
	*i = Int64{
		value:   v,
		defined: true,
	}
}

type Uint struct {
	value   uint
	defined bool
}

func OUint(v uint) (r Uint) {
	r.Set(v)
	return
}
func (u Uint) Get() (v uint, ok bool) {
	if u.defined {
		return u.value, true
	}
	return
}
func (u *Uint) Set(v uint) {
	*u = Uint{
		value:   v,
		defined: true,
	}
}

type Uint8 struct {
	value   uint8
	defined bool
}

func OUint8(v uint8) (r Uint8) {
	r.Set(v)
	return
}
func (u Uint8) Get() (v uint8, ok bool) {
	if u.defined {
		return u.value, true
	}
	return
}
func (u *Uint8) Set(v uint8) {
	*u = Uint8{
		value:   v,
		defined: true,
	}
}

type Uint16 struct {
	value   uint16
	defined bool
}

func OUint16(v uint16) (r Uint16) {
	r.Set(v)
	return
}
func (u Uint16) Get() (v uint16, ok bool) {
	if u.defined {
		return u.value, true
	}
	return
}
func (u *Uint16) Set(v uint16) {
	*u = Uint16{
		value:   v,
		defined: true,
	}
}

type Uint32 struct {
	value   uint32
	defined bool
}

func OUint32(v uint32) (r Uint32) {
	r.Set(v)
	return
}
func (u Uint32) Get() (v uint32, ok bool) {
	if u.defined {
		return u.value, true
	}
	return
}
func (u *Uint32) Set(v uint32) {
	*u = Uint32{
		value:   v,
		defined: true,
	}
}

type Uint64 struct {
	value   uint64
	defined bool
}

func OUint64(v uint64) (r Uint64) {
	r.Set(v)
	return
}
func (u Uint64) Get() (v uint64, ok bool) {
	if u.defined {
		return u.value, true
	}
	return
}
func (u *Uint64) Set(v uint64) {
	*u = Uint64{
		value:   v,
		defined: true,
	}
}

type Float32 struct {
	value   float32
	defined bool
}

func OFloat32(v float32) (r Float32) {
	r.Set(v)
	return
}
func (f Float32) Get() (v float32, ok bool) {
	if f.defined {
		return f.value, true
	}
	return
}
func (f *Float32) Set(v float32) {
	*f = Float32{
		value:   v,
		defined: true,
	}
}

type Float64 struct {
	value   float64
	defined bool
}

func OFloat64(v float64) (r Float64) {
	r.Set(v)
	return
}
func (f Float64) Get() (v float64, ok bool) {
	if f.defined {
		return f.value, true
	}
	return
}
func (f *Float64) Set(v float64) {
	*f = Float64{
		value:   v,
		defined: true,
	}
}

type String struct {
	value   string
	defined bool
}

func OString(v string) (r String) {
	r.Set(v)
	return
}
func (s String) Get() (v string, ok bool) {
	if s.defined {
		return s.value, true
	}
	return
}
func (s *String) Set(v string) {
	*s = String{
		value:   v,
		defined: true,
	}
}
