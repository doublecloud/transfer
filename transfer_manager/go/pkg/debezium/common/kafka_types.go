package common

type KafkaType string

const (
	KafkaTypeInt8    KafkaType = "int8"
	KafkaTypeInt16   KafkaType = "int16"
	KafkaTypeInt32   KafkaType = "int32"
	KafkaTypeInt64   KafkaType = "int64"
	KafkaTypeFloat32 KafkaType = "float"
	KafkaTypeFloat64 KafkaType = "double"
	KafkaTypeBoolean KafkaType = "boolean"
	KafkaTypeString  KafkaType = "string"
	KafkaTypeBytes   KafkaType = "bytes"
	KafkaTypeArray   KafkaType = "array"
	KafkaTypeMap     KafkaType = "map"
	KafkaTypeStruct  KafkaType = "struct"
)
