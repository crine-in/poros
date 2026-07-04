# Poros Developer Reference & API Documentation

Welcome to the **Poros** developer reference manual. This document provides detailed information on internal sharding architecture, eviction policy math, memory layouts for the zero-GC `ByteCache`, and a complete, developer-friendly specification of the REST API endpoints accompanied by copy-pasteable `curl` commands.

---

## 🏛️ System Architecture

Poros is a concurrent, sharded in-memory cache. It splits its internal storage into multiple mutex-protected shards, routing keys dynamically using process-seeded hashing.

```
                              +-----------------------------------+
                              |         Client Request            |
                              +-----------------------------------+
                                                |
                                                v
                              +-----------------------------------+
                              |     Hash Routing (unsafe Cast)    |
                              |   Key -> string/int64 -> uint64   |
                              +-----------------------------------+
                                                |
                                                v
                              +-----------------------------------+
                              |       Idx = Hash & ShardMask      |
                              +-----------------------------------+
                                                |
                      +-------------------------+-------------------------+
                      |                                                   |
                      v                                                   v
           +---------------------+                             +---------------------+
           |      Shard 0        |                             |      Shard N-1      |
           +---------------------+                             +---------------------+
           | - Read/Write Mutex  |                             | - Read/Write Mutex  |
           | - Native Go Map     |                             | - Native Go Map     |
           | - Pluggable Evictor |                             | - Pluggable Evictor |
           | - Expiration Heap   |                             | - Expiration Heap   |
           +---------------------+                             +---------------------+
```

---

## 🏎️ Core Mechanics

### 1. Zero-Allocation Routing
In standard Go, passing a generic key (`K`) into type-independent functions requires casting it to an `any` interface (`any(key)`), which results in a heap allocation due to boxing. 
To achieve sub-nanosecond lookups under load, Poros maps type-specific hasher functions (e.g. `maphash.String` for string keys) to the internal `hashFn` during cache startup using `unsafe.Pointer` casting. This ensures:
- **0 allocations** on reads.
- **0 allocations** on mixed workloads.
- Minimal garbage collection overhead.

### 2. $O(1)$ LFU Eviction Policy
Poros implements LFU eviction using a bucketing system inspired by the $O(1)$ LFU paper:
1. Keys with the same access frequency reside in the same bucket.
2. Buckets are chained together in a doubly-linked frequency list.
3. Accessing a key promotes its entry by moving it to the next frequency bucket in $O(1)$ time.
4. When eviction is triggered, the key is evicted from the tail of the lowest frequency bucket in $O(1)$ time.

---

## 💾 Zero-GC `ByteCache` Layout

For multi-gigabyte cache storage, pointer-rich map entries can overwhelm Go's garbage collector. `ByteCache` addresses this by allocating a single, flat byte array (`[]byte`) per shard and packing entries sequentially:

```
+------------------+------------------+-------------------+--------------------+------------------------+-------------------------+
| entryLen (4B)    | entryHash (8B)   | keyLen (2B)       | valLen (4B)        | expiresAt (8B)         | keyBytes (keyLen)       | valBytes
+------------------+------------------+-------------------+--------------------+------------------------+-------------------------+
```

When writing, if the space at the end of the buffer is insufficient:
1. A dummy gap entry is written at the end.
2. The `writeOffset` wraps back to `0`.
3. If writing overwrites the `readOffset` (oldest record), the oldest entry is automatically evicted, advancing the `readOffset`.

---

## 🌐 HTTP REST API Endpoint Specification

### 1. Set Cache Key (`POST /keys/{key}`)
Writes or overwrites a cache entry. You can supply an optional Time-To-Live (TTL) duration string.

#### Bash `curl` Example
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"value": "crine_session_token_xyz", "ttl": "15m"}' \
  http://127.0.0.1:8080/keys/user_101
```

#### JSON Request Payload
- `value` (Any, Required): The data payload to store (supports strings, numbers, arrays, or objects).
- `ttl` (String, Optional): Duration format (e.g., `10s`, `15m`, `2h`, `0s` = no expiration).

#### Response (`200 OK`)
```json
{
  "status": "success",
  "message": "key set successfully"
}
```

---

### 2. Get Cache Key (`GET /keys/{key}`)
Retrieves a stored value. If the key has expired or does not exist, a `404 Not Found` is returned.

#### Bash `curl` Example
```bash
curl -i http://127.0.0.1:8080/keys/user_101
```

#### Response (`200 OK`)
```json
{
  "key": "user_101",
  "value": "crine_session_token_xyz",
  "ttl_remaining": "14m58.2s"
}
```

#### Error Response (`404 Not Found`)
```json
{
  "status": "error",
  "message": "key not found"
}
```

---

### 3. Delete Cache Key (`DELETE /keys/{key}`)
Removes a key from the cache.

#### Bash `curl` Example
```bash
curl -i -X DELETE http://127.0.0.1:8080/keys/user_101
```

#### Response (`200 OK`)
```json
{
  "status": "success",
  "message": "key deleted"
}
```

#### Error Response (`404 Not Found`)
```json
{
  "status": "error",
  "message": "key not found"
}
```

---

### 4. Increment Counter (`POST /keys/{key}/increment`)
Increments an atomic numeric counter. If the key does not exist, it is initialized to `0` and then incremented.

#### Bash `curl` Example
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"delta": 5}' \
  http://127.0.0.1:8080/keys/page_views/increment
```

#### JSON Request Payload
- `delta` (Integer, Optional): The numeric value to add (defaults to `1` if omitted).

#### Response (`200 OK`)
```json
{
  "key": "page_views",
  "value": 5
}
```

---

### 5. Decrement Counter (`POST /keys/{key}/decrement`)
Decrements an atomic numeric counter.

#### Bash `curl` Example
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"delta": 2}' \
  http://127.0.0.1:8080/keys/page_views/decrement
```

#### JSON Request Payload
- `delta` (Integer, Optional): The numeric value to subtract (defaults to `1` if omitted).

#### Response (`200 OK`)
```json
{
  "key": "page_views",
  "value": 3
}
```

---

### 6. Get Server Metrics (`GET /stats`)
Returns runtime metrics for the cache instance.

#### Bash `curl` Example
```bash
curl -i http://127.0.0.1:8080/stats
```

#### Response (`200 OK`)
```json
{
  "hits": 10243,
  "misses": 341,
  "sets": 1502,
  "evictions": 14,
  "expirations": 42
}
```

---

### 7. Clear Cache (`POST /clear`)
Wipes all keys from the cache.

#### Bash `curl` Example
```bash
curl -i -X POST http://127.0.0.1:8080/clear
```

#### Response (`200 OK`)
```json
{
  "status": "success",
  "message": "cache cleared"
}
```

---

## 💻 Client Integration Examples

### 1. Go (Standard Library)
```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

func main() {
	url := "http://127.0.0.1:8080/keys/my_key"
	payload := []byte(`{"value": "go_developer", "ttl": "5m"}`)

	// POST /set
	resp, _ := http.Post(url, "application/json", bytes.NewBuffer(payload))
	resp.Body.Close()

	// GET
	resp, _ = http.Get(url)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}
```

### 2. Python (Requests)
```python
import requests

url = "http://127.0.0.1:8080/keys/my_key"

# Set Key
response = requests.post(url, json={"value": "python_script", "ttl": "5m"})
print(response.json())

# Get Key
response = requests.get(url)
print(response.json())
```

### 3. Node.js (Fetch API)
```javascript
const url = 'http://127.0.0.1:8080/keys/my_key';

// Set Key
fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ value: 'js_client', ttl: '5m' })
})
.then(res => res.json())
.then(console.log);

// Get Key
fetch(url)
.then(res => res.json())
.then(console.log);
```
