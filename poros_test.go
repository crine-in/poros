package poros

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheBasic(t *testing.T) {
	c := New(Config[string, string]{Shards: 4})
	defer c.Close()

	c.Set("foo", "bar", 0)
	val, ok := c.Get("foo")
	if !ok || val != "bar" {
		t.Errorf("expected bar, got %s (ok=%v)", val, ok)
	}

	c.Delete("foo")
	_, ok = c.Get("foo")
	if ok {
		t.Error("expected foo to be deleted")
	}
}

func TestCacheTTL(t *testing.T) {
	c := New(Config[string, string]{
		DefaultTTL: 50 * time.Millisecond,
	})
	defer c.Close()

	c.SetDefault("foo", "bar")
	val, ok := c.Get("foo")
	if !ok || val != "bar" {
		t.Error("expected foo to be present")
	}

	time.Sleep(100 * time.Millisecond)

	_, ok = c.Get("foo")
	if ok {
		t.Error("expected foo to be expired and cleaned up")
	}
}

func TestCacheTTI(t *testing.T) {
	c := New(Config[string, string]{
		DefaultTTI: 50 * time.Millisecond,
	})
	defer c.Close()

	c.Set("foo", "bar", 0)
	time.Sleep(30 * time.Millisecond)
	// Refresh TTI
	_, ok := c.Get("foo")
	if !ok {
		t.Error("expected foo to be present")
	}

	time.Sleep(30 * time.Millisecond)
	// Refresh TTI again
	_, ok = c.Get("foo")
	if !ok {
		t.Error("expected foo to be present")
	}

	// Wait beyond TTI without accessing
	time.Sleep(100 * time.Millisecond)
	_, ok = c.Get("foo")
	if ok {
		t.Error("expected foo to have expired due to inactivity")
	}
}

func TestCacheEvictionLRU(t *testing.T) {
	evictedCount := int64(0)
	c := New(Config[string, int]{
		Capacity:       3,
		EvictionPolicy: EvictionLRU,
		Shards:         1, // keep to 1 shard for capacity predictability
		OnEvicted: func(k string, v int, reason EvictionReason) {
			if reason == ReasonEvicted {
				atomic.AddInt64(&evictedCount, 1)
			}
		},
	})
	defer c.Close()

	c.SetDefault("a", 1)
	c.SetDefault("b", 2)
	c.SetDefault("c", 3)

	// Access "a" to make it most recently used, "b" is now least recently used
	c.Get("a")

	// Trigger eviction by adding "d"
	c.SetDefault("d", 4)

	_, ok := c.Get("b")
	if ok {
		t.Error("expected key 'b' to be evicted")
	}

	_, ok = c.Get("a")
	if !ok {
		t.Error("expected key 'a' to remain in cache")
	}

	if atomic.LoadInt64(&evictedCount) != 1 {
		t.Errorf("expected 1 eviction, got %d", evictedCount)
	}
}

func TestCacheEvictionFIFO(t *testing.T) {
	c := New(Config[string, int]{
		Capacity:       3,
		EvictionPolicy: EvictionFIFO,
		Shards:         1,
	})
	defer c.Close()

	c.SetDefault("a", 1)
	c.SetDefault("b", 2)
	c.SetDefault("c", 3)

	// Access "a" (does not affect FIFO order)
	c.Get("a")

	// Trigger eviction
	c.SetDefault("d", 4)

	_, ok := c.Get("a")
	if ok {
		t.Error("expected key 'a' to be evicted (first in)")
	}
}

func TestCacheEvictionLFU(t *testing.T) {
	c := New(Config[string, int]{
		Capacity:       3,
		EvictionPolicy: EvictionLFU,
		Shards:         1,
	})
	defer c.Close()

	c.SetDefault("a", 1) // freq = 1
	c.SetDefault("b", 2) // freq = 1
	c.SetDefault("c", 3) // freq = 1

	// Increase freq of a and c
	c.Get("a") // freq = 2
	c.Get("c") // freq = 2

	// Trigger eviction by adding d (freq=1). Key 'b' (freq=1) should be evicted since it has the lowest frequency.
	c.SetDefault("d", 4)

	_, ok := c.Get("b")
	if ok {
		t.Error("expected key 'b' to be evicted")
	}

	_, ok = c.Get("a")
	if !ok {
		t.Error("expected key 'a' to remain")
	}
}

func TestCacheCounters(t *testing.T) {
	t.Run("TypedInt64", func(t *testing.T) {
		c := New(Config[string, int64]{})
		defer c.Close()

		val, err := c.Increment("views", 10)
		if err != nil || val != 10 {
			t.Errorf("expected 10, got %d, err: %v", val, err)
		}

		val, err = c.Increment("views", 5)
		if err != nil || val != 15 {
			t.Errorf("expected 15, got %d, err: %v", val, err)
		}

		val, err = c.Decrement("views", 3)
		if err != nil || val != 12 {
			t.Errorf("expected 12, got %d, err: %v", val, err)
		}
	})

	t.Run("AnyInterface", func(t *testing.T) {
		c := NewAnyCache(Config[string, any]{})
		defer c.Close()

		val, err := c.Increment("counter", 1)
		if err != nil || val != 1 {
			t.Errorf("expected 1, got %v, err: %v", val, err)
		}

		// Set non-numeric type
		c.Set("counter", "hello", 0)
		_, err = c.Increment("counter", 1)
		if err == nil {
			t.Error("expected error when incrementing non-numeric type")
		}
	})
}

func TestSingleflightLoader(t *testing.T) {
	c := New(Config[string, string]{})
	defer c.Close()

	var loadCalls int64
	loader := func(ctx context.Context, key string) (string, error) {
		atomic.AddInt64(&loadCalls, 1)
		time.Sleep(50 * time.Millisecond) // simulate heavy DB load
		return "data_from_db", nil
	}

	var wg sync.WaitGroup
	const concurrentRequests = 10
	wg.Add(concurrentRequests)

	for i := 0; i < concurrentRequests; i++ {
		go func() {
			defer wg.Done()
			val, err := c.GetOrLoad(context.Background(), "user_123", loader)
			if err != nil || val != "data_from_db" {
				t.Errorf("unexpected load results: %v, %v", val, err)
			}
		}()
	}

	wg.Wait()

	calls := atomic.LoadInt64(&loadCalls)
	if calls != 1 {
		t.Errorf("expected only 1 loader call due to singleflight coalescing, got %d", calls)
	}
}

func TestByteCache(t *testing.T) {
	c := NewByteCache(ByteCacheConfig{
		Shards:       2,
		ShardMaxSize: 1024, // 1KB shards
		DefaultTTL:   50 * time.Millisecond,
	})
	defer c.Close()

	// Set & Get
	err := c.Set("key1", []byte("hello world"), 0)
	if err != nil {
		t.Errorf("unexpected set error: %v", err)
	}

	val, err := c.Get("key1")
	if err != nil || string(val) != "hello world" {
		t.Errorf("expected hello world, got %s, err: %v", string(val), err)
	}

	// Delete
	c.Delete("key1")
	_, err = c.Get("key1")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	// TTL
	c.Set("key2", []byte("expires soon"), 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	_, err = c.Get("key2")
	if err != ErrExpired && err != ErrKeyNotFound {
		t.Errorf("expected expiration, got %v", err)
	}

	// Ring buffer wrap & eviction
	largeValue := make([]byte, 400) // ~400 bytes
	c.Set("item1", largeValue, 0)
	c.Set("item2", largeValue, 0)

	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("item_overflow_%d", i), largeValue, 0)
	}

	stats := c.Stats()
	if stats.Sets == 0 {
		t.Error("expected Sets count to be > 0")
	}
}

func TestCacheMemoryAndSizeLimits(t *testing.T) {
	// 1. MaxItemSize
	c := New(Config[string, string]{
		Shards:      1,
		MaxItemSize: 10, // 10 bytes
	})
	defer c.Close()

	c.Set("small", "12345", 0) // 5 bytes
	_, ok := c.Get("small")
	if !ok {
		t.Error("expected small item to be cached")
	}

	c.Set("too_big", "123456789012345", 0) // 15 bytes
	_, ok = c.Get("too_big")
	if ok {
		t.Error("expected too big item to be rejected")
	}

	stats := c.Stats()
	if stats.RejectedSets != 1 {
		t.Errorf("expected 1 rejected set, got %d", stats.RejectedSets)
	}

	// 2. MaxMemory
	c2 := New(Config[string, string]{
		Shards:         1,
		MaxMemory:      20, // 20 bytes total memory
		EvictionPolicy: EvictionLRU,
	})
	defer c2.Close()

	c2.Set("k1", "12345678", 0) // 8 bytes
	c2.Set("k2", "12345678", 0) // 8 bytes
	// current memory = 16 bytes. Fits perfectly.

	c2.Set("k3", "12345678", 0) // 8 bytes -> triggers eviction of k1 (LRU)

	_, ok = c2.Get("k1")
	if ok {
		t.Error("expected key k1 to be evicted to fit memory limit")
	}

	_, ok = c2.Get("k2")
	if !ok {
		t.Error("expected key k2 to remain")
	}
	_, ok = c2.Get("k3")
	if !ok {
		t.Error("expected key k3 to remain")
	}
}
