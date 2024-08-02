package model

import (
	"time"
)

type SerializationFormatName string

const (
	SerializationFormatAuto      = SerializationFormatName("Auto")
	SerializationFormatJSON      = SerializationFormatName("JSON")
	SerializationFormatDebezium  = SerializationFormatName("Debezium")
	SerializationFormatMirror    = SerializationFormatName("Mirror")
	SerializationFormatLbMirror  = SerializationFormatName("LbMirror")
	SerializationFormatNative    = SerializationFormatName("Native")
	SerializationFormatRawColumn = SerializationFormatName("RawColumn")

	ColumnNameParamName = "column_name"
)

type Batching struct {
	Enabled        bool
	Interval       time.Duration
	MaxChangeItems int
	MaxMessageSize int64
}

type SerializationFormat struct {
	Name             SerializationFormatName
	Settings         map[string]string
	SettingsKV       [][2]string
	BatchingSettings *Batching
}

func (f *SerializationFormat) Copy() *SerializationFormat {
	result := &SerializationFormat{
		Name:             f.Name,
		Settings:         make(map[string]string, len(f.Settings)),
		SettingsKV:       make([][2]string, len(f.SettingsKV)),
		BatchingSettings: nil,
	}
	for k, v := range f.Settings {
		result.Settings[k] = v
	}
	for i, kv := range f.SettingsKV {
		result.SettingsKV[i] = [2]string{kv[0], kv[1]}
	}
	if f.BatchingSettings != nil {
		result.BatchingSettings = &Batching{
			Enabled:        f.BatchingSettings.Enabled,
			Interval:       f.BatchingSettings.Interval,
			MaxChangeItems: f.BatchingSettings.MaxChangeItems,
			MaxMessageSize: f.BatchingSettings.MaxMessageSize,
		}
	}
	return result
}
