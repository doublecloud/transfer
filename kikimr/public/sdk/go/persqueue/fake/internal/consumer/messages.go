// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package consumer

import (
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/model"
)

type ReadSessionProps struct {
	Topics              []string
	SessionID           string
	ClientID            string
	ExplicitPartitions  bool
	WaitCommitOnRelease bool
}

// Requests

type Request interface {
	_isConsumerRequest()
}

type CommitReq struct {
	Cookies []int
}

func (r *CommitReq) _isConsumerRequest() {}

type ReadReq struct {
	MaxMessages int
}

func (r *ReadReq) _isConsumerRequest() {}

type StartReadReq struct {
	Topic        string
	Partition    uint32
	Generation   uint64
	ReadOffset   uint64
	CommitOffset uint64
	VerifyOffset bool
}

func (r *StartReadReq) _isConsumerRequest() {}

// Responses

type Response interface {
	_isConsumerResponse()
}

type MessagesResp struct {
	Cookie   int
	Messages []*data.Message
}

func (r *MessagesResp) _isConsumerResponse() {}

type CommitResp struct {
	Cookies []int
}

func (r *CommitResp) _isConsumerResponse() {}

type LockResp struct {
	Generation uint64
	Partition  *model.PartitionInfo
}

func (r *LockResp) _isConsumerResponse() {}

type ReleaseResp struct {
	Generation uint64
	Topic      string
	Partition  uint32
	MustCommit bool
}

func (r *ReleaseResp) _isConsumerResponse() {}
