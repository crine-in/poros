package poros

import (
	"context"
	"errors"
	"time"
)

// EvictionType defines the eviction policy type.
type EvictionType int

const (
	EvictionLRU EvictionType = iota
	EvictionLFU
	EvictionFIFO
	EvictionNone
)

// EvictionReason describes why an entry was removed.
type EvictionReason int

const (
	ReasonExpired EvictionReason = iota
	ReasonEvicted                // capacity limit reached
	ReasonDeleted                // manual deletion
	ReasonUpdated                // set overwrite
)

func (r EvictionReason) String() string {
	switch r {
	case ReasonExpired:
		return "expired"
	case ReasonEvicted:
		return "evicted"
	case ReasonDeleted:
		return "deleted"
	case ReasonUpdated:
		return "updated"
	default:
		return "unknown"
	}
}

var (
	ErrNotNumeric = errors.New("value is not a numeric type")
	ErrClosed     = errors.New("cache is closed")
)

// Stats tracks cache metrics.
type Stats struct {
	Hits       int64 `json:"hits"`
	Misses     int64 `json:"misses"`
	Sets       int64 `json:"sets"`
	Evictions  int64 `json:"evictions"`
	Expirations int64 `json:"expirations"`
}

// Config represents cache configuration.
type Config[K comparable, V any] struct {
	Shards            int
	DefaultTTL        time.Duration
	DefaultTTI        time.Duration
	EvictionPolicy    EvictionType
	Capacity          int // max items per cache (total capacity)
	JanitorInterval   time.Duration
	OnEvicted         func(key K, val V, reason EvictionReason)
	Loader            func(ctx context.Context, key K) (V, error)
}

// Cache defines the operations available on the in-memory cache.
type Cache[K comparable, V any] interface {
	Get(key K) (V, bool)
	GetWithTTL(key K) (V, time.Duration, bool)
	Set(key K, val V, ttl time.Duration)
	SetDefault(key K, val V)
	Delete(key K) bool
	Clear()
	Len() int
	Stats() Stats

	// Atomic Counters
	Increment(key K, delta int64) (int64, error)
	Decrement(key K, delta int64) (int64, error)

	// Singleflight loading
	GetOrLoad(ctx context.Context, key K, loader func(ctx context.Context, key K) (V, error)) (V, error)

	// Close releases resources (stops janitor goroutines)
	Close() error
}
