package poros

import (
	"context"
	"fmt"
	"hash/maphash"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/crine-in/poros/singleflight"
)

type cacheImpl[K comparable, V any] struct {
	shards      []*shard[K, V]
	shardMask   uint64
	seed        maphash.Seed
	config      Config[K, V]
	stats       Stats
	stopJanitor chan struct{}
	closeOnce   sync.Once
	loaderGroup singleflight.Group[K, V]
	hashFn      func(K) uint64
}

// AnyCache is a helper type for a cache with string keys and any value.
type AnyCache = Cache[string, any]

// NewAnyCache creates a new Cache with string keys and interface/any values.
func NewAnyCache(config Config[string, any]) AnyCache {
	return New[string, any](config)
}

// New creates a new sharded cache instance with type parameters and a custom configuration.
func New[K comparable, V any](config Config[K, V]) Cache[K, V] {
	if config.Shards <= 0 {
		config.Shards = nextPowerOfTwo(runtime.NumCPU() * 4)
	}
	if config.JanitorInterval <= 0 {
		config.JanitorInterval = 1 * time.Minute
	}

	shardCount := nextPowerOfTwo(config.Shards)

	c := &cacheImpl[K, V]{
		shards:      make([]*shard[K, V], shardCount),
		shardMask:   uint64(shardCount - 1),
		seed:        maphash.MakeSeed(),
		config:      config,
		stopJanitor: make(chan struct{}),
	}

	capacityPerShard := 0
	if config.Capacity > 0 {
		capacityPerShard = (config.Capacity + shardCount - 1) / shardCount
	}

	for i := 0; i < shardCount; i++ {
		c.shards[i] = newShard[K, V](
			capacityPerShard,
			config.EvictionPolicy,
			config.DefaultTTL,
			config.DefaultTTI,
			config.OnEvicted,
			&c.stats,
		)
	}

	c.startJanitor()

	// Zero-allocation generic hashing setup
	var hashFn func(K) uint64
	var zeroK K
	switch any(zeroK).(type) {
	case string:
		h := func(key string) uint64 {
			return maphash.String(c.seed, key)
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case int:
		h := func(key int) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case int64:
		h := func(key int64) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case int32:
		h := func(key int32) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case int16:
		h := func(key int16) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case int8:
		h := func(key int8) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case uint:
		h := func(key uint) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case uint64:
		h := func(key uint64) uint64 {
			return mix(key)
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case uint32:
		h := func(key uint32) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case uint16:
		h := func(key uint16) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case uint8:
		h := func(key uint8) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	case uintptr:
		h := func(key uintptr) uint64 {
			return mix(uint64(key))
		}
		hashFn = *(*func(K) uint64)(unsafe.Pointer(&h))
	default:
		hashFn = func(key K) uint64 {
			return maphash.String(c.seed, fmt.Sprintf("%v", key))
		}
	}
	c.hashFn = hashFn

	return c
}

func (c *cacheImpl[K, V]) getShard(key K) *shard[K, V] {
	hash := c.hashFn(key)
	idx := hash & c.shardMask
	return c.shards[idx]
}

func (c *cacheImpl[K, V]) Get(key K) (V, bool) {
	return c.getShard(key).get(key, time.Now())
}

func (c *cacheImpl[K, V]) GetWithTTL(key K) (V, time.Duration, bool) {
	return c.getShard(key).getWithTTL(key, time.Now())
}

func (c *cacheImpl[K, V]) Set(key K, val V, ttl time.Duration) {
	c.getShard(key).set(key, val, ttl, time.Now())
}

func (c *cacheImpl[K, V]) SetDefault(key K, val V) {
	c.getShard(key).set(key, val, 0, time.Now())
}

func (c *cacheImpl[K, V]) Delete(key K) bool {
	return c.getShard(key).delete(key)
}

func (c *cacheImpl[K, V]) Clear() {
	for _, s := range c.shards {
		s.clear()
	}
}

func (c *cacheImpl[K, V]) Len() int {
	total := 0
	for _, s := range c.shards {
		total += s.len()
	}
	return total
}

func (c *cacheImpl[K, V]) Stats() Stats {
	return Stats{
		Hits:        atomic.LoadInt64(&c.stats.Hits),
		Misses:      atomic.LoadInt64(&c.stats.Misses),
		Sets:        atomic.LoadInt64(&c.stats.Sets),
		Evictions:   atomic.LoadInt64(&c.stats.Evictions),
		Expirations: atomic.LoadInt64(&c.stats.Expirations),
	}
}

func (c *cacheImpl[K, V]) GetOrLoad(ctx context.Context, key K, loader func(ctx context.Context, key K) (V, error)) (V, error) {
	if val, ok := c.Get(key); ok {
		return val, nil
	}

	loadFn := loader
	if loadFn == nil {
		loadFn = c.config.Loader
	}

	if loadFn == nil {
		var zero V
		return zero, fmt.Errorf("no loader function provided")
	}

	val, err := c.loaderGroup.Do(key, func() (V, error) {
		// Double check cache under the write barrier or after wait
		if val, ok := c.Get(key); ok {
			return val, nil
		}

		res, err := loadFn(ctx, key)
		if err != nil {
			return res, err
		}

		c.SetDefault(key, res)
		return res, nil
	})

	return val, err
}

func (c *cacheImpl[K, V]) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopJanitor)
	})
	return nil
}

func (c *cacheImpl[K, V]) startJanitor() {
	if c.config.JanitorInterval <= 0 {
		return
	}
	ticker := time.NewTicker(c.config.JanitorInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.cleanExpired()
			case <-c.stopJanitor:
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *cacheImpl[K, V]) cleanExpired() {
	now := time.Now()
	for _, s := range c.shards {
		s.cleanExpired(now)
	}
}

func nextPowerOfTwo(v int) int {
	if v <= 1 {
		return 1
	}
	return 1 << (bits.Len(uint(v - 1)))
}

func hashKey[K comparable](key K, seed maphash.Seed) uint64 {
	switch k := any(key).(type) {
	case string:
		return maphash.String(seed, k)
	case int:
		return mix(uint64(k))
	case int64:
		return mix(uint64(k))
	case int32:
		return mix(uint64(k))
	case int16:
		return mix(uint64(k))
	case int8:
		return mix(uint64(k))
	case uint:
		return mix(uint64(k))
	case uint64:
		return mix(k)
	case uint32:
		return mix(uint64(k))
	case uint16:
		return mix(uint64(k))
	case uint8:
		return mix(uint64(k))
	case uintptr:
		return mix(uint64(k))
	default:
		return maphash.String(seed, fmt.Sprintf("%v", k))
	}
}

func mix(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
