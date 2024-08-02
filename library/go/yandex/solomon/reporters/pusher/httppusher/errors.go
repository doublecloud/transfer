package httppusher

import "errors"

var (
	ErrSendGatewayTimeout = errors.New("cannot send metrics due to gateway timeout")
	ErrEmptyCluster       = errors.New("empty cluster not allowed")
	ErrEmptyProject       = errors.New("empty project not allowed")
	ErrEmptyService       = errors.New("empty service not allowed")
)
