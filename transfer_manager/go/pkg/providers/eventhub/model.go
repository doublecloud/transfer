package eventhub

import (
	"time"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
)

const (
	EventHubAuthSAS = "SAS"
	ProviderType    = abstract.ProviderType("eventhub")
)

var _ server.Source = (*EventHubSource)(nil)

type EventHubSource struct {
	NamespaceName     string
	HubName           string
	ConsumerGroup     string
	Topic             string
	StartingOffset    string
	StartingTimeStamp *time.Time
	Auth              *EventHubAuth
	Transformer       *server.DataTransformOptions

	ParserConfig map[string]interface{}
}

var _ server.Source = (*EventHubSource)(nil)

type EventHubAuth struct {
	Method, KeyName string
	KeyValue        server.SecretString
}

func (s *EventHubSource) WithDefaults() {
	if s.Auth == nil {
		s.Auth = &EventHubAuth{
			Method:   EventHubAuthSAS,
			KeyName:  "",
			KeyValue: "",
		}
	}
	if s.StartingOffset == "" {
		s.StartingOffset = "-1"
	}
	if s.ConsumerGroup == "" {
		s.ConsumerGroup = "$Default"
	}
	if s.Transformer != nil && s.Transformer.CloudFunction == "" {
		s.Transformer = nil
	}
}

func (s *EventHubSource) IsSource() {
}

func (s *EventHubSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *EventHubSource) Validate() error {
	if s.ParserConfig != nil {
		parserConfigStruct, err := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if err != nil {
			return xerrors.Errorf("unable to create new parser config, err: %w", err)
		}
		return parserConfigStruct.Validate()
	}
	return nil
}

func (s *EventHubSource) IsAppendOnly() bool {
	if s.ParserConfig == nil {
		return false
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if parserConfigStruct == nil {
			return false
		}
		return parserConfigStruct.IsAppendOnly()
	}
}

func (s *EventHubSource) IsDefaultMirror() bool {
	return s.ParserConfig == nil
}

func (s *EventHubSource) Parser() map[string]interface{} {
	return s.ParserConfig
}
