package action

import (
	"encoding/json"
	"net/url"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

type Container interface {
	Wrap() *Single
	JSON() (string, error)
}

type FileAction interface {
	Container
	PathAsURI() (*url.URL, error)
	IsDataChanged() bool
}

func New(raw string) (Container, error) {
	action := new(Single)
	if err := json.Unmarshal([]byte(raw), action); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal action: %w", err)
	}

	return action.Unwrap(), nil
}

func jsonString(a Container) (string, error) {
	b, err := json.Marshal(a.Wrap())
	if err != nil {
		return "", xerrors.Errorf("unable to unmarshal action: %w", err)
	}
	return string(b), nil
}

type Single struct {
	Txn        *SetTransaction `json:"txn,omitempty"`
	Add        *AddFile        `json:"add,omitempty"`
	Remove     *RemoveFile     `json:"remove,omitempty"`
	MetaData   *Metadata       `json:"metaData,omitempty"`
	Protocol   *Protocol       `json:"protocol,omitempty"`
	Cdc        *AddCDCFile     `json:"cdc,omitempty"`
	CommitInfo *CommitInfo     `json:"commitInfo,omitempty"`
}

func (s *Single) Unwrap() Container {
	switch {
	case s.Add != nil:
		return s.Add
	case s.Remove != nil:
		return s.Remove
	case s.MetaData != nil:
		return s.MetaData
	case s.Txn != nil:
		return s.Txn
	case s.Protocol != nil:
		return s.Protocol
	case s.Cdc != nil:
		return s.Cdc
	case s.CommitInfo != nil:
		return s.CommitInfo
	default:
		return nil
	}
}
