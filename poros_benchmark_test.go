package poros

import (
	"fmt"
	"sync"
	"testing"
)

// SingleMutexMap is a standard map wrapped with a single RWMutex, used for benchmark comparison.
type SingleMutexMap struct {
	mu sync.RWMutex
	m  map[string]any
}

func NewSingleMutexMap() *SingleMutexMap {
	return &SingleMutexMap{
		m: make(map[string]any),
	}
}

func (s *SingleMutexMap) Get(key string) (any, bool) {
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *SingleMutexMap) Set(key string, val any) {
	s.mu.Lock()
	s.m[key] = val
	s.mu.Unlock()
}

func BenchmarkGet(b *testing.B) {
	const keyCount = 10000
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.Run("Poros", func(b *testing.B) {
		c := New(Config[string, any]{Shards: 64})
		defer c.Close()
		for i := 0; i < keyCount; i++ {
			c.Set(keys[i], i, 0)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				c.Get(keys[i%keyCount])
				i++
			}
		})
	})

	b.Run("SyncMap", func(b *testing.B) {
		var m sync.Map
		for i := 0; i < keyCount; i++ {
			m.Store(keys[i], i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Load(keys[i%keyCount])
				i++
			}
		})
	})

	b.Run("SingleMutex", func(b *testing.B) {
		m := NewSingleMutexMap()
		for i := 0; i < keyCount; i++ {
			m.Set(keys[i], i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Get(keys[i%keyCount])
				i++
			}
		})
	})
}

func BenchmarkSet(b *testing.B) {
	const keyCount = 10000
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.Run("Poros", func(b *testing.B) {
		c := New(Config[string, any]{Shards: 64})
		defer c.Close()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				c.Set(keys[i%keyCount], i, 0)
				i++
			}
		})
	})

	b.Run("SyncMap", func(b *testing.B) {
		var m sync.Map
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(keys[i%keyCount], i)
				i++
			}
		})
	})

	b.Run("SingleMutex", func(b *testing.B) {
		m := NewSingleMutexMap()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Set(keys[i%keyCount], i)
				i++
			}
		})
	})
}

func BenchmarkMixed90Read10Write(b *testing.B) {
	const keyCount = 10000
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.Run("Poros", func(b *testing.B) {
		c := New(Config[string, any]{Shards: 64})
		defer c.Close()
		for i := 0; i < keyCount; i++ {
			c.Set(keys[i], i, 0)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%keyCount]
				if i%10 == 0 {
					c.Set(key, i, 0)
				} else {
					c.Get(key)
				}
				i++
			}
		})
	})

	b.Run("SyncMap", func(b *testing.B) {
		var m sync.Map
		for i := 0; i < keyCount; i++ {
			m.Store(keys[i], i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%keyCount]
				if i%10 == 0 {
					m.Store(key, i)
				} else {
					m.Load(key)
				}
				i++
			}
		})
	})

	b.Run("SingleMutex", func(b *testing.B) {
		m := NewSingleMutexMap()
		for i := 0; i < keyCount; i++ {
			m.Set(keys[i], i)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%keyCount]
				if i%10 == 0 {
					m.Set(key, i)
				} else {
					m.Get(key)
				}
				i++
			}
		})
	})
}
