package lru

import (
	"container/list"
)

// Cache declare interface for use cache
type Cache interface {
	Add(key, value interface{})
	Get(key interface{}) (interface{}, bool)
	Remove(key interface{}) (interface{}, bool)
}

func New(maxSize int) Cache {
	if maxSize <= 0 {
		return nopCache{}
	}
	return &cache{
		maxSize: maxSize,
		list:    list.New(),
		index:   make(map[interface{}]*list.Element),
	}
}

type nopCache struct{}

func (nopCache) Add(interface{}, interface{})           {}
func (nopCache) Get(interface{}) (interface{}, bool)    { return nil, false }
func (nopCache) Remove(interface{}) (interface{}, bool) { return nil, false }

type cache struct {
	maxSize int
	list    *list.List
	index   map[interface{}]*list.Element
}

func (c *cache) Add(key, value interface{}) {
	if el, has := c.index[key]; has {
		c.list.MoveToFront(el)
		el.Value.(*entry).value = value
		return
	}
	if c.list.Len() == c.maxSize {
		c.evictBack()
	}
	c.index[key] = c.list.PushFront(&entry{key, value})
}

func (c *cache) Get(key interface{}) (interface{}, bool) {
	el, has := c.index[key]
	if !has {
		return nil, false
	}
	c.list.MoveToFront(el)
	return el.Value.(*entry).value, true
}

func (c *cache) Remove(key interface{}) (interface{}, bool) {
	el, has := c.index[key]
	if !has {
		return nil, false
	}
	delete(c.index, key)
	return c.list.Remove(el).(*entry).value, true
}

func (c *cache) Size() int {
	return c.list.Len()
}

func (c *cache) Purge() {
	c.list.Init()
	c.index = nil
}

func (c *cache) evictBack() {
	el := c.list.Back()
	c.list.Remove(el)
	delete(c.index, el.Value.(*entry).key)
}

type entry struct {
	key, value interface{}
}
