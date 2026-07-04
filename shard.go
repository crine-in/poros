package poros

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/crine/poros/policy"
)

type entry[V any] struct {
	value      V
	expiresAt  time.Time
	lastAccess time.Time
}

func (e *entry[V]) isExpired(now time.Time, defaultTTI time.Duration) bool {
	if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
		return true
	}
	if defaultTTI > 0 && !e.lastAccess.IsZero() && now.Sub(e.lastAccess) > defaultTTI {
		return true
	}
	return false
}

type shard[K comparable, V any] struct {
	mu          sync.RWMutex
	items       map[K]*entry[V]
	policy      policy.Policy[K]
	capacity    int
	defaultTTL  time.Duration
	defaultTTI  time.Duration
	onEvictedCb func(key K, val V, reason EvictionReason)
	stats       *Stats
}

func newShard[K comparable, V any](capacity int, policyType EvictionType, defaultTTL, defaultTTI time.Duration, onEvicted func(key K, val V, reason EvictionReason), stats *Stats) *shard[K, V] {
	var pol policy.Policy[K]
	switch policyType {
	case EvictionLRU:
		pol = policy.NewLRU[K]()
	case EvictionLFU:
		pol = policy.NewLFU[K]()
	case EvictionFIFO:
		pol = policy.NewFIFO[K]()
	}

	return &shard[K, V]{
		items:       make(map[K]*entry[V]),
		policy:      pol,
		capacity:    capacity,
		defaultTTL:  defaultTTL,
		defaultTTI:  defaultTTI,
		onEvictedCb: onEvicted,
		stats:       stats,
	}
}

func (s *shard[K, V]) get(key K, now time.Time) (V, bool) {
	var zero V

	// Check if read lock path is sufficient (no active LRU/LFU changes or TTI updates)
	hasActivePolicy := s.policy != nil
	_, isFIFO := s.policy.(*policy.FIFO[K])
	needsWrite := (hasActivePolicy && !isFIFO) || s.defaultTTI > 0

	if !needsWrite {
		s.mu.RLock()
		e, exists := s.items[key]
		if !exists {
			s.mu.RUnlock()
			atomic.AddInt64(&s.stats.Misses, 1)
			return zero, false
		}
		if e.isExpired(now, s.defaultTTI) {
			s.mu.RUnlock()
			s.deleteExpired(key, now)
			atomic.AddInt64(&s.stats.Misses, 1)
			return zero, false
		}
		val := e.value
		s.mu.RUnlock()
		atomic.AddInt64(&s.stats.Hits, 1)
		return val, true
	}

	// Write path for LRU/LFU/TTI update
	s.mu.Lock()
	defer s.mu.Unlock()

	e, exists := s.items[key]
	if !exists {
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, false
	}

	if e.isExpired(now, s.defaultTTI) {
		s.remove(key, ReasonExpired)
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, false
	}

	e.lastAccess = now
	if s.policy != nil {
		s.policy.OnAccess(key)
	}
	atomic.AddInt64(&s.stats.Hits, 1)
	return e.value, true
}

func (s *shard[K, V]) getWithTTL(key K, now time.Time) (V, time.Duration, bool) {
	var zero V
	s.mu.Lock()
	defer s.mu.Unlock()

	e, exists := s.items[key]
	if !exists {
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, 0, false
	}

	if e.isExpired(now, s.defaultTTI) {
		s.remove(key, ReasonExpired)
		atomic.AddInt64(&s.stats.Misses, 1)
		return zero, 0, false
	}

	e.lastAccess = now
	if s.policy != nil {
		s.policy.OnAccess(key)
	}

	var remaining time.Duration
	if !e.expiresAt.IsZero() {
		remaining = e.expiresAt.Sub(now)
	} else if s.defaultTTI > 0 {
		remaining = s.defaultTTI - now.Sub(e.lastAccess)
	}

	atomic.AddInt64(&s.stats.Hits, 1)
	return e.value, remaining, true
}

func (s *shard[K, V]) set(key K, val V, ttl time.Duration, now time.Time) {
	s.mu.Lock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	} else if s.defaultTTL > 0 && ttl == 0 {
		expiresAt = now.Add(s.defaultTTL)
	}

	var callbackValue V
	var callbackTrigger bool
	var callbackReason EvictionReason

	e, exists := s.items[key]
	if exists {
		callbackValue = e.value
		callbackTrigger = true
		callbackReason = ReasonUpdated

		e.value = val
		e.expiresAt = expiresAt
		e.lastAccess = now
		if s.policy != nil {
			s.policy.OnInsert(key)
		}
		atomic.AddInt64(&s.stats.Sets, 1)
		s.mu.Unlock()

		if callbackTrigger && s.onEvictedCb != nil {
			s.onEvictedCb(key, callbackValue, callbackReason)
		}
		return
	}

	// Enforce capacity limit
	if s.capacity > 0 && len(s.items) >= s.capacity {
		if s.policy != nil {
			if evictKey, ok := s.policy.Evict(); ok {
				if evEntry, ok := s.items[evictKey]; ok {
					callbackValue = evEntry.value
					callbackTrigger = true
					callbackReason = ReasonEvicted
					s.remove(evictKey, ReasonEvicted)
				}
			}
		} else {
			// Evict first key from map iteration if no policy is set
			for evictKey, evEntry := range s.items {
				callbackValue = evEntry.value
				callbackTrigger = true
				callbackReason = ReasonEvicted
				s.remove(evictKey, ReasonEvicted)
				break
			}
		}
	}

	s.items[key] = &entry[V]{
		value:      val,
		expiresAt:  expiresAt,
		lastAccess: now,
	}

	if s.policy != nil {
		s.policy.OnInsert(key)
	}
	atomic.AddInt64(&s.stats.Sets, 1)
	s.mu.Unlock()

	if callbackTrigger && s.onEvictedCb != nil {
		s.onEvictedCb(key, callbackValue, callbackReason)
	}
}

func (s *shard[K, V]) delete(key K) bool {
	s.mu.Lock()
	val, ok := s.remove(key, ReasonDeleted)
	s.mu.Unlock()

	if ok && s.onEvictedCb != nil {
		s.onEvictedCb(key, val, ReasonDeleted)
	}
	return ok
}

// remove removes the entry from the map. Assumes lock is held.
func (s *shard[K, V]) remove(key K, reason EvictionReason) (V, bool) {
	var zero V
	e, exists := s.items[key]
	if !exists {
		return zero, false
	}
	delete(s.items, key)
	if s.policy != nil {
		s.policy.OnRemove(key)
	}

	switch reason {
	case ReasonEvicted:
		atomic.AddInt64(&s.stats.Evictions, 1)
	case ReasonExpired:
		atomic.AddInt64(&s.stats.Expirations, 1)
	}
	return e.value, true
}

func (s *shard[K, V]) deleteExpired(key K, now time.Time) {
	s.mu.Lock()
	e, exists := s.items[key]
	var callbackValue V
	var callbackTrigger bool
	if exists && e.isExpired(now, s.defaultTTI) {
		callbackValue = e.value
		callbackTrigger = true
		s.remove(key, ReasonExpired)
	}
	s.mu.Unlock()

	if callbackTrigger && s.onEvictedCb != nil {
		s.onEvictedCb(key, callbackValue, ReasonExpired)
	}
}

func (s *shard[K, V]) clear() {
	s.mu.Lock()
	var itemsToNotify []struct {
		key K
		val V
	}

	if s.onEvictedCb != nil {
		for k, e := range s.items {
			itemsToNotify = append(itemsToNotify, struct {
				key K
				val V
			}{k, e.value})
		}
	}

	s.items = make(map[K]*entry[V])
	if s.policy != nil {
		s.policy.Clear()
	}
	s.mu.Unlock()

	if s.onEvictedCb != nil {
		for _, item := range itemsToNotify {
			s.onEvictedCb(item.key, item.val, ReasonDeleted)
		}
	}
}

func (s *shard[K, V]) len() int {
	s.mu.RLock()
	length := len(s.items)
	s.mu.RUnlock()
	return length
}

func (s *shard[K, V]) cleanExpired(now time.Time) {
	s.mu.Lock()
	var expiredKeys []K
	for k, e := range s.items {
		if e.isExpired(now, s.defaultTTI) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	var evictedItems []struct {
		key K
		val V
	}
	for _, k := range expiredKeys {
		if val, ok := s.remove(k, ReasonExpired); ok {
			evictedItems = append(evictedItems, struct {
				key K
				val V
			}{k, val})
		}
	}
	s.mu.Unlock()

	if s.onEvictedCb != nil && len(evictedItems) > 0 {
		for _, item := range evictedItems {
			s.onEvictedCb(item.key, item.val, ReasonExpired)
		}
	}
}
