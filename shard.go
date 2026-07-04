package poros

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crine-in/poros/policy"
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
	mu            sync.RWMutex
	items         map[K]*entry[V]
	policy        policy.Policy[K]
	capacity      int
	maxItemSize   int64
	maxMemory     int64
	currentMemory int64
	defaultTTL    time.Duration
	defaultTTI    time.Duration
	onEvictedCb   func(key K, val V, reason EvictionReason)
	stats         *Stats
}

func newShard[K comparable, V any](capacity int, maxItemSize, maxMemory int64, policyType EvictionType, defaultTTL, defaultTTI time.Duration, onEvicted func(key K, val V, reason EvictionReason), stats *Stats) *shard[K, V] {
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
		maxItemSize: maxItemSize,
		maxMemory:   maxMemory,
		defaultTTL:  defaultTTL,
		defaultTTI:  defaultTTI,
		onEvictedCb: onEvicted,
		stats:       stats,
	}
}

func sizeOf(v any) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case []byte:
		return int64(len(val))
	case int:
		return 8
	case int64:
		return 8
	case uint64:
		return 8
	case float64:
		return 8
	case int32:
		return 4
	case uint32:
		return 4
	case float32:
		return 4
	case int16:
		return 2
	case uint16:
		return 2
	case int8:
		return 1
	case uint8:
		return 1
	case bool:
		return 1
	default:
		// Fallback to JSON length approximation
		if data, err := json.Marshal(v); err == nil {
			return int64(len(data))
		}
		return 0
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

func (s *shard[K, V]) evictToFit(requiredSize int64, now time.Time) {
	for s.currentMemory+requiredSize > s.maxMemory && len(s.items) > 0 {
		if !s.evictOne(ReasonEvicted) {
			break
		}
	}
}

func (s *shard[K, V]) evictOne(reason EvictionReason) bool {
	var evictKey K
	var ok bool
	if s.policy != nil {
		evictKey, ok = s.policy.Evict()
	} else {
		// Fallback to first map key
		for k := range s.items {
			evictKey = k
			ok = true
			break
		}
	}

	if ok {
		if evEntry, exists := s.items[evictKey]; exists {
			evSize := sizeOf(evEntry.value)
			s.currentMemory -= evSize
			atomic.AddInt64(&s.stats.MemoryBytes, -evSize)
			val, _ := s.remove(evictKey, reason)
			if s.onEvictedCb != nil {
				s.onEvictedCb(evictKey, val, reason)
			}
			return true
		}
	}
	return false
}

func (s *shard[K, V]) set(key K, val V, ttl time.Duration, now time.Time) {
	newSize := sizeOf(val)
	if s.maxItemSize > 0 && newSize > s.maxItemSize {
		atomic.AddInt64(&s.stats.RejectedSets, 1)
		return
	}
	if s.maxMemory > 0 && newSize > s.maxMemory {
		atomic.AddInt64(&s.stats.RejectedSets, 1)
		return
	}

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

		oldSize := sizeOf(e.value)
		// Update memory tracker (subtract old size, add new size)
		s.currentMemory -= oldSize
		atomic.AddInt64(&s.stats.MemoryBytes, -oldSize)

		// Check if the updated item exceeds memory limit
		if s.maxMemory > 0 && s.currentMemory+newSize > s.maxMemory {
			// Try to evict elements to make room
			s.evictToFit(newSize, now)
		}

		// Recheck if it fits now
		if s.maxMemory > 0 && s.currentMemory+newSize > s.maxMemory {
			// If it still doesn't fit, we reject the update (and restore memory stats)
			s.currentMemory += oldSize
			atomic.AddInt64(&s.stats.MemoryBytes, oldSize)
			atomic.AddInt64(&s.stats.RejectedSets, 1)
			s.mu.Unlock()
			return
		}

		e.value = val
		e.expiresAt = expiresAt
		e.lastAccess = now
		s.currentMemory += newSize
		atomic.AddInt64(&s.stats.MemoryBytes, newSize)

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

	// For a new item: evict to fit capacity limit AND memory limit
	if s.capacity > 0 && len(s.items) >= s.capacity {
		s.evictOne(ReasonEvicted)
	}

	if s.maxMemory > 0 && s.currentMemory+newSize > s.maxMemory {
		s.evictToFit(newSize, now)
	}

	// Recheck if it fits now
	if s.maxMemory > 0 && s.currentMemory+newSize > s.maxMemory {
		// If it still doesn't fit, reject
		atomic.AddInt64(&s.stats.RejectedSets, 1)
		s.mu.Unlock()
		return
	}

	s.items[key] = &entry[V]{
		value:      val,
		expiresAt:  expiresAt,
		lastAccess: now,
	}
	s.currentMemory += newSize
	atomic.AddInt64(&s.stats.MemoryBytes, newSize)

	if s.policy != nil {
		s.policy.OnInsert(key)
	}
	atomic.AddInt64(&s.stats.Sets, 1)
	s.mu.Unlock()
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

	// Subtract memory
	itemSize := sizeOf(e.value)
	s.currentMemory -= itemSize
	atomic.AddInt64(&s.stats.MemoryBytes, -itemSize)

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

	// Reset memory
	atomic.AddInt64(&s.stats.MemoryBytes, -s.currentMemory)
	s.currentMemory = 0

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
