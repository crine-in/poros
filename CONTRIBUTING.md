# Contributing to Poros

Thank you for your interest in contributing to **Poros**! As an essential infrastructure component at crine-in, we maintain high standards for performance, lock safety, and allocation-free mechanics.

---

## 🛠️ Developer Setup

### Prerequisites

- **Go**: Version 1.20 or newer (built and validated with Go 1.24).
- **Git**: Version control setup.

### Getting the Code

Clone the repository:

```bash
git clone https://github.com/crine-in/poros.git
cd poros
```

---

## 📐 Coding Guidelines

1. **Idiomatic Go**: Follow standards set by [Effective Go](https://go.dev/doc/effective_go) and [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
2. **Type Safety & Generics**: Keep cache methods generic where possible, but optimize string key hot-paths to ensure 0 allocations.
3. **No External Dependencies**: To keep Poros ultra-lightweight and secure, we avoid external third-party dependencies. All HTTP server routing and singleflight operations must use standard library primitives.

---

## 🧪 Testing and Benchmarking

Before proposing any changes, verify that your updates pass all unit tests and do not introduce performance regressions or memory allocations.

### Run Unit Tests (with Race Detector)

Always run tests with the `-race` detector enabled to verify concurrency safety:

```bash
go test -v -race ./...
```

### Run Performance Benchmarks

If you modify code in `cache.go`, `shard.go`, or the eviction policies, verify that throughput and memory statistics do not degrade:

```bash
go test -bench=. -benchmem
```

Verify that the `allocs/op` for `BenchmarkGet` and `BenchmarkMixed` remains at `0` for string-keyed configs.

---

## 🧹 Code Quality Checks

We use native Go tools to verify style and formatting:

- **Formatting**: Format code using `gofmt`:
  ```bash
  gofmt -s -w .
  ```
- **Static Analysis**: Verify correctness with `go vet`:
  ```bash
  go vet ./...
  ```

---

## 📥 Submission Process

1. **Branch Naming**: Use a prefix describing your change (e.g. `feature/add-prometheus-metrics` or `bugfix/lfu-eviction-nil-pointer`).
2. **Commit Messages**: Write clear, descriptive commit messages starting with a verb (e.g. `feat: implement graceful HTTP shutdown` or `fix: resolve race in active janitor sweep`).
3. **Open a PR**: Describe your changes, performance impacts (include a before/after benchmark comparison if applicable), and verification steps.
