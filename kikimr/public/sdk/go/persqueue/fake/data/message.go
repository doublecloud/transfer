// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package data

import "time"

type WriteMessage struct {
	Created time.Time
	SeqNo   uint64
	Data    []byte
}

type Message struct {
	WriteMessage
	Partition uint32
	Offset    int
	Source    string
	Topic     string
}
