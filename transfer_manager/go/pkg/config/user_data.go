package config

import (
	"errors"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/instanceutil"
)

var (
	UserData map[string]interface{}

	ErrNoSuchField = errors.New("no such field")
)

func fetchUserData() (map[string]interface{}, error) {
	userData := map[string]interface{}{}
	err := instanceutil.GetGoogleCEUserData(&userData)
	return userData, err
}

func getUserDataField(name string) (interface{}, error) {
	if UserData == nil {
		var err error
		UserData, err = fetchUserData()
		if err != nil {
			return nil, xerrors.Errorf("cannot fetch user data: %w", err)
		}
	}
	value, ok := UserData[name]
	if !ok {
		return nil, ErrNoSuchField
	}
	return value, nil
}
