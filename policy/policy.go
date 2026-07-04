package policy

// Policy defines the interface for eviction policies.
// All methods are expected to be fast, ideally O(1).
type Policy[K comparable] interface {
	// OnAccess is called when a key is retrieved from the cache.
	OnAccess(key K)

	// OnInsert is called when a new key is added to the cache.
	OnInsert(key K)

	// OnRemove is called when a key is manually deleted or overwritten.
	OnRemove(key K)

	// Evict returns a key that should be evicted according to the policy,
	// and removes it from the policy tracking. Returns false if policy is empty.
	Evict() (K, bool)

	// Len returns the number of tracked elements in the policy.
	Len() int

	// Clear resets the policy tracking state.
	Clear()
}
