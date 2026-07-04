# Poros Technical Documentation

This document describes the design, implementation, and API specifications of the **Poros** high-performance in-memory cache library.

---

## 🏛️ System Architecture

Poros is structured to balance low-latency operations with flexible lifecycle management (expirations and evictions). The core component, `Cache[K, V]`, scales concurrently using an internal array of shards:

```
                  +-----------------------------------+
                  |           poros.Cache             |
                  +-----------------------------------+
                                    |
                                    v (Hash Router: Hash(Key) & Mask)
     +------------------------------+------------------------------+
     |                              |                              |
     v                              v                              v
+---------+                    +---------+                    +---------+
| Shard 0 |                    | Shard 1 |                    | Shard N |
+---------+                    +---------+                    +---------+
| - Mutex |                    | - Mutex |                    | - Mutex |
| - Map   |                    | - Map   |                    | - Map   |
| - LRU/  |                    | - LRU/  |                    | - LRU/  |
|   LFU   |                    |   LFU   |                    |   LFU   |
+---------+                    +---------+                    +---------+
```

---

## ⚙️ Core Components

### 1. Hash Routing and Sharding
- **Process-Seeded Hash**: Hashing uses Go's `hash/maphash` package initialized with a process-lifetime seed (`maphash.MakeSeed()`). This provides robust resistance against Hash-DoS vulnerability attacks where malicious actors attempt to feed keys that trigger map collisions.
- **Power-of-Two Shards**: The number of shards is always forced to a power of two. This optimizes key-to-shard routing by replacing the slow modulo operation (`hash % shards`) with a fast bitwise AND (`hash & mask`).
- **Zero-Allocation Routing**: When creating a cache, a type-assertion router converts function pointers at compile time. This permits passing type-safe keys (like `string` or `int64`) directly to native hashers, bypassing interface wrapper heap allocations (`any(key)` boxing).

### 2. Eviction Policies
Eviction algorithms reside under the `policy/` package and implement the `Evictor[K]` interface.
- **LRU (Least Recently Used)**: Utilizes a custom doubly-linked list. Every read or write promotes the element to the front. The element at the tail is evicted when the capacity limit is reached.
- **FIFO (First In First Out)**: Uses a queue built from a doubly-linked list. Elements are pushed to the back during insertion and popped from the front during eviction. Reads do not alter queue ordering.
- **$O(1)$ LFU (Least Frequently Used)**: Implemented using a bucketing algorithm inspired by the "An $O(1)$ LFU Cache Eviction Algorithm" paper. Keys are grouped into frequency nodes linked in a master list. Accessing a key promotes it to a node with a higher frequency count. If the bucket list has no higher node, one is created. Eviction pops elements from the lowest frequency node. This operates in constant $O(1)$ time for reads, writes, and evictions.

### 3. TTL / TTI Sliding Expiration
- **Time-To-Live (TTL)**: Fixed expiration. An entry is expired after a static duration from its write time.
- **Time-To-Idle (TTI)**: Sliding window expiration. An entry's expiration time is pushed forward upon read or update.
- **Lazy Eviction**: Expired keys are checked on-access (`Get`) and removed immediately if they have expired.
- **Active Janitor Sweeper**: A background cleaner goroutine ticks at a regular interval (`JanitorInterval`), locking individual shards one-by-one to sweep and delete expired keys, keeping memory footprints stable.

---

## ⚡ Zero-GC `ByteCache` Ring Buffer

Standard Go maps store pointers. When a map grows to millions of entries, the Go garbage collector spends significant CPU time scanning the map pointers during GC cycles.

`ByteCache` solves this by storing raw byte payloads inside a single pre-allocated flat byte array ring buffer (`[]byte`) per shard:

```
ByteCache Shard Buffer Layout:
+------------------+------------------+-------------------+--------------------+------------------------+-------------------------+
| entryLen (4B)    | entryHash (8B)   | keyLen (2B)       | valLen (4B)        | expiresAt (8B)         | keyBytes (keyLen)       | ...
+------------------+------------------+-------------------+--------------------+------------------------+-------------------------+
```

### Memory Wrapping & Eviction Mechanics:
- When a new byte slice is set, `ByteCache` writes the layout metadata, the key bytes, and value bytes sequentially at `writeOffset`.
- If the entry does not fit at the end of the buffer, `writeOffset` wraps back to the beginning (`0`), writing a dummy marker at the trailing gap.
- During writes, `ByteCache` checks if the new write block overlaps with `readOffset` (which points to the oldest stored entry). If it overlaps, it automatically evicts the oldest entry, moving `readOffset` forward, until enough space is cleared.

---

## 🌐 HTTP REST API Endpoint Specification

The HTTP Cache Service exposes a REST API to manage cache entries.

### 1. Set Cache Key
- **Method / Path**: `POST /keys/{key}`
- **Request Headers**: `Content-Type: application/json`
- **Request Body**:
  ```json
  {
    "value": "user_session_token_123",
    "ttl": "15m"
  }
  ```
- **Response**: `200 OK`
  ```json
  {
    "status": "success",
    "message": "key set successfully"
  }
  ```

### 2. Get Cache Key
- **Method / Path**: `GET /keys/{key}`
- **Response**: `200 OK`
  ```json
  {
    "key": "session_id",
    "value": "user_session_token_123",
    "ttl_remaining": "14m52s"
  }
  ```
- **Error Responses**:
  - `404 Not Found` if key does not exist or has expired.

### 3. Delete Cache Key
- **Method / Path**: `DELETE /keys/{key}`
- **Response**: `200 OK`
  ```json
  {
    "status": "success",
    "message": "key deleted"
  }
  ```

### 4. Increment Counter
- **Method / Path**: `POST /keys/{key}/increment`
- **Request Body**:
  ```json
  {
    "delta": 5
  }
  ```
- **Response**: `200 OK`
  ```json
  {
    "key": "page_hits",
    "value": 15
  }
  ```

### 5. Decrement Counter
- **Method / Path**: `POST /keys/{key}/decrement`
- **Request Body**:
  ```json
  {
    "delta": 2
  }
  ```
- **Response**: `200 OK`
  ```json
  {
    "key": "active_users",
    "value": 88
  }
  ```

### 6. Get Metrics / Stats
- **Method / Path**: `GET /stats`
- **Response**: `200 OK`
  ```json
  {
    "hits": 10582,
    "misses": 412,
    "sets": 1209,
    "evictions": 54,
    "expirations": 103
  }
  ```

### 7. Clear Cache
- **Method / Path**: `POST /clear`
- **Response**: `200 OK`
  ```json
  {
    "status": "success",
    "message": "cache cleared"
  }
  ```
