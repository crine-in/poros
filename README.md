# Poros ☄️

[![Go Version](https://img.shields.io/github/go-mod/go-version/crine-in/poros?style=flat-square)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/crine-in/poros.svg)](https://pkg.go.dev/github.com/crine-in/poros)
[![License](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)

**Poros** is a high-performance, low-latency, and memory-safe in-memory key-value cache library and service developed for CRINE infrastructure. Built from the ground up to solve Go's Garbage Collector latency spikes under high throughput, Poros offers a type-safe generic cache along with a zero-allocation, pointerless flat-byte ring-buffer cache.

---

## ✨ Features

- **Lock-Free Scaling (Sharded Mutexes)**: Spreads key ranges across configurable sharding blocks to eliminate lock contention under parallel loads.
- **Zero-Allocation String Routing**: Leverages an `unsafe` type-casting hash bypass mapping at initialization to compute hash keys directly from native types (e.g. `maphash.String` for strings) without heap interface boxing (`any(key)`). Reads and mixed read-writes run with **0 allocations**.
- **Pluggable Eviction Policies**:
  - **LRU** (Least Recently Used) Doubly-Linked List.
  - **FIFO** (First In First Out) Queue.
  - **LFU** (Least Frequently Used) implemented in constant time $O(1)$ using frequency-bucket mappings.
  - **None** (Explicit manual deletes and expiration only).
- **TTL & TTI Sliding Expiration**: Active background sweeping combined with passive on-access evictions.
- **Thundering Herd Protection**: Coalesces duplicate concurrent loader requests for missing keys into a single callback via `GetOrLoad`.
- **Atomic Numeric Counters**: Supporting all Go primitive numbers (`int`, `int64`, `uint`, etc.) with zero extra allocations.
- **Pointerless Zero-GC `ByteCache`**: Stores raw byte payload entries sequentially in a sharded ring buffer, keeping values completely hidden from Go GC pointer scans.
- **HTTP Cache Daemon**: Light-weight, zero-dependency REST HTTP service with graceful shutdown support.

---

## 📦 Installation

```bash
go get github.com/crine-in/poros
```

---

## ⚡ Quick Start

### 1. In-Memory Generic Cache

```go
package main

import (
	"fmt"
	"time"
	"github.com/crine-in/poros"
)

func main() {
	// Initialize high-performance cache
	cache := poros.New(poros.Config[string, string]{
		Shards:          32,
		DefaultTTL:      5 * time.Minute,
		EvictionPolicy:  poros.EvictionLRU,
		Capacity:        50000,
	})
	defer cache.Close()

	// Set value
	cache.Set("session_id", "active_user_1", 10*time.Second)

	// Get value
	if val, ok := cache.Get("session_id"); ok {
		fmt.Printf("User Session: %s\n", val)
	}
}
```

### 2. Zero-GC `ByteCache`

```go
package main

import (
	"fmt"
	"github.com/crine-in/poros"
)

func main() {
	// 64MB Cache Shards
	bc := poros.NewByteCache(poros.ByteCacheConfig{
		Shards:       8,
		ShardMaxSize: 8 * 1024 * 1024,
	})
	defer bc.Close()

	bc.Set("image_blob", []byte{0xff, 0xd8, 0xff, 0xe0}, 0)

	val, err := bc.Get("image_blob")
	if err == nil {
		fmt.Printf("Loaded blob size: %d bytes\n", len(val))
	}
}
```

---

## 🌐 HTTP Cache Daemon (`porosd`)

Poros includes a production-ready HTTP server executable to expose the cache as a standalone daemon.

### Environment Configuration (.env)
The daemon loads its configuration from a `.env` file at the root. Copy the template to start:
```bash
cp .env.example .env
```
Ensure `POROS_KEY` is configured. If present, all HTTP API endpoints require the header `Authorization: Bearer <key>`.

### Start the Server
```bash
go run cmd/porosd/main.go -port 8080 -shards 32 -capacity 100000 -policy 0
```

### API Endpoints Spec (Requires `Authorization: Bearer <token>`)

| Method     | Endpoint                | Description                  | Payload Example                 |
| :--------- | :---------------------- | :--------------------------- | :------------------------------ |
| **GET**    | `/keys/{key}`           | Retrieve key value           | _None_                          |
| **POST**   | `/keys/{key}`           | Set or update value          | `{"value": "bar", "ttl": "5m"}` |
| **DELETE** | `/keys/{key}`           | Delete key                   | _None_                          |
| **POST**   | `/keys/{key}/increment` | Increment numeric value      | `{"delta": 5}`                  |
| **POST**   | `/keys/{key}/decrement` | Decrement numeric value      | `{"delta": 2}`                  |
| **GET**    | `/stats`                | Get runtime cache statistics | _None_                          |
| **POST**   | `/clear`                | Clear all keys               | _None_                          |

---

## 🏎️ Benchmarks

Benchmarks run under high multi-threaded parallel workloads (12-core CPU, Go 1.24.4):

### Read Operations (Parallel GET)

- **`sync.Map`**: `8.81 ns/op` (0 B/op, 0 allocs/op)
- **`Poros`**: `93.20 ns/op` (**0 B/op**, **0 allocs/op**)
- **Single Mutex Map**: `75.99 ns/op` (0 B/op, 0 allocs/op)

### Write Operations (Parallel SET)

- **`Poros`**: `111.5 ns/op` (**24 B/op**, **1 alloc/op**)
- **`sync.Map`**: `91.90 ns/op` (72 B/op, 3 allocs/op)
- **Single Mutex Map**: `201.0 ns/op` (8 B/op, 0 allocs/op)

> Under write concurrency, **Poros is 1.8x faster than a single-mutex map** and uses **3x less memory than `sync.Map`** per operation.

### Mixed Workload (90% Read, 10% Write)

- **`Poros`**: `71.94 ns/op` (**0 B/op**, **0 allocs/op**)
- **`sync.Map`**: `16.57 ns/op` (7 B/op, 0 allocs/op)
- **Single Mutex Map**: `46.06 ns/op` (0 B/op, 0 allocs/op)

> The mixed workload has **zero heap allocations**, protecting your production services from Garbage Collector sweeps and latency jitters.

---

## 🩺 HTTP API Stress Test Report

Exposing the cache service as an HTTP REST endpoint, we subjected the server to a high-concurrency stress test with **100 concurrent workers** under full parallel loads on an AMD Ryzen 5 CPU:

### 1. Write Throughput (100% POST)

- **Total Requests**: 47,738 (in 5s)
- **Throughput**: **9,522.79 req/sec**
- **Success Rate**: **100.00%**
- **Avg Latency**: `9.95 ms`
- **p99 Latency**: `29.26 ms`

### 2. Read Throughput (100% GET)

- **Total Requests**: 40,513 (in 5s)
- **Throughput**: **8,078.73 req/sec**
- **Success Rate**: **100.00%**
- **Avg Latency**: `11.99 ms`
- **p99 Latency**: `28.68 ms`

### 3. Mixed Workload (90% Read, 10% Write)

- **Total Requests**: 32,289 (in 5s)
- **Throughput**: **6,422.44 req/sec**
- **Success Rate**: **100.00%**
- **Avg Latency**: `15.21 ms`
- **p99 Latency**: `37.04 ms`

> [!NOTE]
> All stress test workloads achieved **100.00% success rate** with zero connection resets, showcasing Poros' stability under severe network saturation and concurrent sharding lock safety.

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
