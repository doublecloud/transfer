package common

import ytschema "go.ytsaurus.tech/yt/go/schema"

type YTTypeStorer interface {
	YTType() string
}

type YTTypeInt8 struct{}

func (t *YTTypeInt8) YTType() string {
	return string(ytschema.TypeInt8)
}

type YTTypeUint8 struct{}

func (t *YTTypeUint8) YTType() string {
	return string(ytschema.TypeUint8)
}

type YTTypeInt16 struct{}

func (t *YTTypeInt16) YTType() string {
	return string(ytschema.TypeInt16)
}

type YTTypeUint16 struct{}

func (t *YTTypeUint16) YTType() string {
	return string(ytschema.TypeUint16)
}

type YTTypeInt32 struct{}

func (t *YTTypeInt32) YTType() string {
	return string(ytschema.TypeInt32)
}

type YTTypeUint32 struct{}

func (t *YTTypeUint32) YTType() string {
	return string(ytschema.TypeUint32)
}

type YTTypeInt64 struct{}

func (t *YTTypeInt64) YTType() string {
	return string(ytschema.TypeInt64)
}

type YTTypeUint64 struct{}

func (t *YTTypeUint64) YTType() string {
	return string(ytschema.TypeUint64)
}

type YTTypeBoolean struct{}

func (t *YTTypeBoolean) YTType() string {
	return string(ytschema.TypeBoolean)
}

type YTTypeBytes struct{}

func (t *YTTypeBytes) YTType() string {
	return string(ytschema.TypeBytes)
}

type YTTypeString struct{}

func (t *YTTypeString) YTType() string {
	return string(ytschema.TypeString)
}

type YTTypeFloat32 struct{}

func (t *YTTypeFloat32) YTType() string {
	return string(ytschema.TypeFloat32)
}

type YTTypeFloat64 struct{}

func (t *YTTypeFloat64) YTType() string {
	return string(ytschema.TypeFloat64)
}

type YTTypeAny struct{}

func (t *YTTypeAny) YTType() string {
	return string(ytschema.TypeAny)
}

type YTTypeDate struct{}

func (t *YTTypeDate) YTType() string {
	return string(ytschema.TypeDate)
}

type YTTypeDateTime struct{}

func (t *YTTypeDateTime) YTType() string {
	return string(ytschema.TypeDatetime)
}

type YTTypeTimestamp struct{}

func (t *YTTypeTimestamp) YTType() string {
	return string(ytschema.TypeTimestamp)
}

type YTTypeInterval struct{}

func (t *YTTypeInterval) YTType() string {
	return string(ytschema.TypeInterval)
}
