package singleflight

import (
	"sync"
)

type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group[K, V]) Do(key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[V])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call[V])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	c.wg.Done()
	return c.val, c.err
}
