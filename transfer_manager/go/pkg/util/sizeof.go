package util

import (
	"reflect"
)

func DeepSizeof(v interface{}) uint64 {
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Float32, reflect.Float64:
		return uint64(r.Type().Size())
	case reflect.String:
		return uint64(r.Type().Size()) + uint64(r.Len())
	case reflect.Ptr:
		if r.IsNil() {
			return uint64(r.Type().Size())
		}
		return uint64(r.Type().Size()) + DeepSizeof(r.Elem())
	case reflect.Struct:
		return SizeOfStruct(v)
	case reflect.Slice, reflect.Array:
		return sizeofSlice(v)
	case reflect.Map:
		return sizeofMap(v)
	case reflect.Interface:
		if r.IsNil() {
			return uint64(r.Type().Size())
		}
		return uint64(r.Type().Size()) + DeepSizeof(r.Interface())
	case reflect.Invalid:
		return 0
	default:
		return uint64(r.Type().Size())
	}
}

func SizeOfStruct(v interface{}) uint64 {
	s := reflect.ValueOf(v)

	var size uint64
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.CanInterface() {
			if f.Kind() == reflect.Interface {
				size += uint64(f.Type().Size())
			}
			size += DeepSizeof(f.Interface())
		} else {
			size += uint64(f.Type().Size())
		}
	}
	return size
}

func sizeofSlice(v interface{}) uint64 {
	s := reflect.ValueOf(v)

	elemType := s.Type().Elem()
	switch elemType.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Float32, reflect.Float64:
		// if the type is simple, you can just count them as simple as that
		return uint64(s.Type().Size()) + uint64(s.Len())*uint64(elemType.Size())
	default:
	}

	size := uint64(s.Type().Size())
	for i := 0; i < s.Len(); i++ {
		f := s.Index(i)
		if f.Kind() == reflect.Interface {
			size += uint64(f.Type().Size())
		}
		size += DeepSizeof(f.Interface())
	}
	return size
}

func sizeofMap(v interface{}) uint64 {
	m := reflect.ValueOf(v)

	size := uint64(m.Type().Size())
	iterator := m.MapRange()
	for iterator.Next() {
		v := iterator.Value()
		size += DeepSizeof(iterator.Key().Interface())
		if v.Type().Kind() == reflect.Interface {
			size += uint64(v.Type().Size())
		}
		size += DeepSizeof(v.Interface())
	}
	return size
}
