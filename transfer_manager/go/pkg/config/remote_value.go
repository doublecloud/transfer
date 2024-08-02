package config

import (
	"os"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
)

// go-sumtype:decl RemoteValue
type RemoteValue interface {
	TypeTagged
	isRemoteValue()
	String() string
}

var (
	_ RemoteValue = (*PlainText)(nil)
	_ RemoteValue = (*LocalFile)(nil)
)

type PlainText struct {
	Value string `mapstructure:"value"`
}

func (p PlainText) IsTypeTagged()  {}
func (p PlainText) isRemoteValue() {}

func (p PlainText) String() string {
	return p.Value
}

type LocalFile struct {
	FilePath string `mapstructure:"file_path"`
}

func (l LocalFile) IsTypeTagged()  {}
func (l LocalFile) isRemoteValue() {}

func (l LocalFile) String() string {
	return ""
}

func init() {
	RegisterTypeTagged((*RemoteValue)(nil), (*PlainText)(nil), "plain_text", nil)
	RegisterTypeTagged((*RemoteValue)(nil), (*LocalFile)(nil), "local_file", func(tagged TypeTagged) (interface{}, error) {
		path := tagged.(*LocalFile).FilePath
		data, err := os.ReadFile(path)
		if err != nil {
			logger.Log.Warnf("unable to read: %s", path)
			return &PlainText{Value: ""}, nil
		}
		return &PlainText{Value: string(data)}, nil
	})
}
