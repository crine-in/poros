package poros

import (
	"encoding/binary"
	"errors"
	"hash/maphash"
	"runtime"
	"sync"
	"time"
)

var (
	ErrEntryTooLarge = errors.New("entry is too large for the shard buffer")
	ErrKeyNotFound   = errors.New("key not found in byte cache")
	ErrExpired       = errors.New("key has expired")
)

// ByteCacheConfig holds configuration for the zero-GC ByteCache.
type ByteCacheConfig struct {
	Shards          int
	ShardMaxSize    int           // size in bytes of each shard buffer
	DefaultTTL      time.Duration
	JanitorInterval time.Duration
}

// ByteCache is a concurrent, sharded, zero-GC key-value cache for raw byte slices.
type ByteCache struct {
	shards      []*byteCacheShard
	shardMask   uint64
	seed        maphash.Seed
	config      ByteCacheConfig
	stats       Stats
	stopJanitor chan struct{}
	closeOnce   sync.Once
}

type byteCacheShard struct {
	mu          sync.RWMutex
	buf         []byte
	writeOffset int
	readOffset  int
	m           map[uint64]int
	defaultTTL  time.Duration
	stats       *Stats
}

// NewByteCache creates a new zero-GC ByteCache instance.
func NewByteCache(config ByteCacheConfig) *ByteCache {
	if config.Shards <= 0 {
		config.Shards = nextPowerOfTwo(runtime.NumCPU() * 4)
	} else {
		config.Shards = nextPowerOfTwo(config.Shards)
	}
	if config.ShardMaxSize <= 0 {
		config.ShardMaxSize = 16 * 1024 * 1024 // 16MB default
	}
	if config.JanitorInterval <= 0 {
		config.JanitorInterval = 1 * time.Minute
	}

	shardCount := config.Shards
	c := &ByteCache{
		shards:      make([]*byteCacheShard, shardCount),
		shardMask:   uint64(shardCount - 1),
		seed:        maphash.MakeSeed(),
		config:      config,
		stopJanitor: make(chan struct{}),
	}

	for i := 0; i < shardCount; i++ {
		c.shards[i] = &byteCacheShard{
			buf:        make([]byte, config.ShardMaxSize),
			m:          make(map[uint64]int),
			defaultTTL: config.DefaultTTL,
			stats:      &c.stats,
		}
	}

	c.startJanitor()

	return c
}

func (c *ByteCache) getShard(key string) (*byteCacheShard, uint64) {
	hash := maphash.String(c.seed, key)
	idx := hash & c.shardMask
	return c.shards[idx], hash
}

// Set stores a byte slice value associated with a key string.
func (c *ByteCache) Set(key string, val []byte, ttl time.Duration) error {
	shard, hash := c.getShard(key)
	return shard.set(key, hash, val, ttl, time.Now())
}

// Get retrieves the byte slice associated with a key string.
func (c *ByteCache) Get(key string) ([]byte, error) {
	shard, hash := c.getShard(key)
	return shard.get(key, hash, time.Now())
}

// Delete removes a key string from the cache.
func (c *ByteCache) Delete(key string) bool {
	shard, hash := c.getShard(key)
	return shard.delete(hash)
}

// Close stops background routines and releases resources.
func (c *ByteCache) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopJanitor)
	})
	return nil
}

func (c *ByteCache) startJanitor() {
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

func (c *ByteCache) cleanExpired() {
	now := time.Now()
	for _, s := range c.shards {
		s.cleanExpired(now)
	}
}

func (c *ByteCache) Stats() Stats {
	return Stats{
		Hits:        c.stats.Hits,
		Misses:      c.stats.Misses,
		Sets:        c.stats.Sets,
		Evictions:   c.stats.Evictions,
		Expirations: c.stats.Expirations,
	}
}

func (s *byteCacheShard) set(key string, hash uint64, val []byte, ttl time.Duration, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt int64 = 0
	if ttl > 0 {
		expiresAt = now.Add(ttl).UnixNano()
	} else if s.defaultTTL > 0 {
		expiresAt = now.Add(s.defaultTTL).UnixNano()
	}

	keyBytes := []byte(key)
	entryLen := 26 + len(keyBytes) + len(val)

	if entryLen > len(s.buf) {
		return ErrEntryTooLarge
	}

	// If entry doesn't fit at the end of the buffer, wrap to the beginning.
	if s.writeOffset+entryLen > len(s.buf) {
		// Fill remaining space with a dummy entry
		gapLen := len(s.buf) - s.writeOffset
		if gapLen >= 4 {
			binary.BigEndian.PutUint32(s.buf[s.writeOffset:s.writeOffset+4], uint32(gapLen))
			binary.BigEndian.PutUint64(s.buf[s.writeOffset+4:s.writeOffset+12], 0) // hash = 0 indicates dummy gap
		}

		// Evict overlapping entries from [0, entryLen]
		for s.readOffset != s.writeOffset && s.overlaps(0, entryLen) {
			s.evictOldest()
		}
		s.writeOffset = 0
	}

	// Evict overlapping entries in current write range [writeOffset, writeOffset + entryLen]
	for s.readOffset != s.writeOffset && s.overlaps(s.writeOffset, entryLen) {
		s.evictOldest()
	}

	off := s.writeOffset
	binary.BigEndian.PutUint32(s.buf[off:off+4], uint32(entryLen))
	binary.BigEndian.PutUint64(s.buf[off+4:off+12], hash)
	binary.BigEndian.PutUint16(s.buf[off+12:off+14], uint16(len(keyBytes)))
	binary.BigEndian.PutUint32(s.buf[off+14:off+18], uint32(len(val)))
	binary.BigEndian.PutUint64(s.buf[off+18:off+26], uint64(expiresAt))
	copy(s.buf[off+26:off+26+len(keyBytes)], keyBytes)
	copy(s.buf[off+26+len(keyBytes):off+entryLen], val)

	s.m[hash] = off
	s.writeOffset += entryLen
	s.stats.Sets++

	return nil
}

func (s *byteCacheShard) get(key string, hash uint64, now time.Time) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	off, exists := s.m[hash]
	if !exists {
		s.stats.Misses++
		return nil, ErrKeyNotFound
	}

	entryLen := int(binary.BigEndian.Uint32(s.buf[off : off+4]))
	entryHash := binary.BigEndian.Uint64(s.buf[off+4 : off+12])

	if entryHash != hash {
		s.stats.Misses++
		return nil, ErrKeyNotFound
	}

	keyLen := int(binary.BigEndian.Uint16(s.buf[off+12 : off+14]))
	valLen := int(binary.BigEndian.Uint32(s.buf[off+14 : off+18]))
	expiresAt := int64(binary.BigEndian.Uint64(s.buf[off+18 : off+26]))

	if expiresAt > 0 && now.UnixNano() > expiresAt {
		s.stats.Misses++
		return nil, ErrExpired
	}

	storedKey := string(s.buf[off+26 : off+26+keyLen])
	if storedKey != key {
		s.stats.Misses++
		return nil, ErrKeyNotFound
	}

	val := make([]byte, valLen)
	copy(val, s.buf[off+26+keyLen:off+entryLen])

	s.stats.Hits++
	return val, nil
}

func (s *byteCacheShard) delete(hash uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m[hash]
	if !exists {
		return false
	}
	delete(s.m, hash)
	return true
}

func (s *byteCacheShard) overlaps(start int, length int) bool {
	end := start + length
	// Check if readOffset falls inside the write range.
	// Since we evict entry by entry, readOffset points to the oldest entry.
	// If it overlaps, evicting the oldest entry shifts readOffset.
	if s.readOffset < s.writeOffset {
		return s.readOffset >= start && s.readOffset < end
	}
	if s.readOffset > s.writeOffset {
		// Read offset has wrapped around write offset.
		// Overlap occurs if readOffset is between start and end.
		return s.readOffset >= start && s.readOffset < end
	}
	return false
}

func (s *byteCacheShard) evictOldest() {
	off := s.readOffset
	entryLen := int(binary.BigEndian.Uint32(s.buf[off : off+4]))
	hash := binary.BigEndian.Uint64(s.buf[off+4 : off+12])

	if hash != 0 {
		if s.m[hash] == off {
			delete(s.m, hash)
			s.stats.Evictions++
		}
	}

	s.readOffset += entryLen
	if s.readOffset >= len(s.buf) {
		s.readOffset = 0
	}
}

func (s *byteCacheShard) cleanExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for hash, off := range s.m {
		expiresAt := int64(binary.BigEndian.Uint64(s.buf[off+18 : off+26]))
		if expiresAt > 0 && now.UnixNano() > expiresAt {
			delete(s.m, hash)
			s.stats.Expirations++
		}
	}
}
