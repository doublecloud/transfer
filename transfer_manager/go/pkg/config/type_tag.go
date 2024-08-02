package config

import (
	"fmt"
	"reflect"
)

type typeTag string
type typeTaggedInterface reflect.Type
type typeTaggedImplementation reflect.Type

type TypeTagged interface {
	IsTypeTagged()
}

type registryEntry struct {
	implType      typeTaggedImplementation
	postprocessor func(TypeTagged) (interface{}, error)
}

var (
	typeTagRegistry = map[typeTaggedInterface]map[typeTag]registryEntry{}
)

func isTypeTaggedInterface(typ reflect.Type) bool {
	if typ.Kind() != reflect.Interface {
		return false
	}
	typeTaggedInterface := reflect.TypeOf((*TypeTagged)(nil)).Elem()
	return typ.Implements(typeTaggedInterface)
}

func RegisterTypeTagged(iface interface{}, impl TypeTagged, tag string, postprocessor func(TypeTagged) (interface{}, error)) {
	ifaceType := reflect.TypeOf(iface).Elem()
	implType := reflect.TypeOf(impl).Elem()
	tagMap, ok := typeTagRegistry[ifaceType]
	if !ok {
		tagMap = map[typeTag]registryEntry{}
		typeTagRegistry[ifaceType] = tagMap
	}
	if existingEntry, ok := tagMap[typeTag(tag)]; ok {
		panic(fmt.Sprintf(
			"Tag %s for interface %s registered twice: conflicting implementations are %s and %s",
			tag,
			ifaceType.Name(),
			implType.Name(),
			existingEntry.implType.Name(),
		))
	}
	if implType.Kind() != reflect.Struct {
		panic("Type-tagged interface implementation must be a pointer to a struct")
	}

	tagMap[typeTag(tag)] = registryEntry{
		implType:      implType,
		postprocessor: postprocessor,
	}
}
