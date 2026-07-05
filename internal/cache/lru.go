package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key     K
	value   V
	element *list.Element
	expiry  time.Time
}

// LRU is a fixed-size least-recently-used cache.
type LRU[K comparable, V any] struct {
	mu    sync.Mutex
	cap   int
	items map[K]*entry[K, V]
	list  *list.List
}

// New creates a new LRU cache with the given capacity.
func New[K comparable, V any](cap int) *LRU[K, V] {
	if cap <= 0 {
		cap = 1
	}
	return &LRU[K, V]{
		cap:   cap,
		items: make(map[K]*entry[K, V], cap),
		list:  list.New(),
	}
}

// Set adds or updates a key with an optional TTL.
func (c *LRU[K, V]) Set(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.items == nil {
		c.items = make(map[K]*entry[K, V], c.cap)
	}
	if c.list == nil {
		c.list = list.New()
	}

	if ent, ok := c.items[key]; ok {
		ent.value = value
		if ttl > 0 {
			ent.expiry = time.Now().Add(ttl)
		} else {
			ent.expiry = time.Time{}
		}
		c.list.MoveToFront(ent.element)
		return
	}

	if c.list.Len() >= c.cap {
		c.evictOldestLocked()
	}

	ent := &entry[K, V]{
		key:   key,
		value: value,
	}
	if ttl > 0 {
		ent.expiry = time.Now().Add(ttl)
	}
	ent.element = c.list.PushFront(ent)
	c.items[key] = ent
}

// Get retrieves a value and marks it most recently used.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	ent, ok := c.items[key]
	if !ok {
		return zero, false
	}
	if !ent.expiry.IsZero() && time.Now().After(ent.expiry) {
		c.removeLocked(ent)
		return zero, false
	}
	c.list.MoveToFront(ent.element)
	return ent.value, true
}

// Delete removes a key from the cache.
func (c *LRU[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		c.removeLocked(ent)
	}
}

// Len returns the current number of entries.
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *LRU[K, V]) evictOldestLocked() {
	back := c.list.Back()
	if back == nil {
		return
	}
	if ent, ok := back.Value.(*entry[K, V]); ok {
		c.removeLocked(ent)
	}
}

func (c *LRU[K, V]) removeLocked(ent *entry[K, V]) {
	if ent == nil {
		return
	}
	delete(c.items, ent.key)
	if ent.element != nil {
		c.list.Remove(ent.element)
		ent.element = nil
	}
}
