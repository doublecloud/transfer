// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package concurrent

import (
	"fmt"
	"sync"
)

type Key = interface{}
type Value = interface{}

// Map is thread-safe with atomic GetOrCreate method
type Map struct {
	m  map[Key]Value
	mu sync.RWMutex
}

func NewMap() *Map {
	return &Map{m: make(map[Key]Value)}
}

func (m *Map) Get(k Key) Value {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.m[k]
}

func (m *Map) PutIfAbsent(k Key, v Value) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.m[k]; ok {
		return false
	}
	m.m[k] = v
	return true
}

func (m *Map) GetOrCreate(k Key, creator func(Key) Value) Value {
	res, err := m.GetOrCreateWithErr(k, func(key Key) (value Value, err error) {
		return creator(key), nil
	})
	if err != nil {
		panic(fmt.Errorf("should never happen: %v", err))
	}
	return res
}

func (m *Map) GetOrCreateWithErr(k Key, creator func(Key) (Value, error)) (Value, error) {
	m.mu.RLock()
	res, ok := m.m[k]
	m.mu.RUnlock()
	if ok {
		return res, nil
	}
	m.mu.Lock()
	res, ok = m.m[k]
	var err error
	if !ok {
		res, err = creator(k)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		m.m[k] = res
	}
	m.mu.Unlock()
	return res, nil
}
