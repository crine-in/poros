package policy

type lruNode[K comparable] struct {
	key  K
	prev *lruNode[K]
	next *lruNode[K]
}

// LRU implements the Least Recently Used eviction policy.
type LRU[K comparable] struct {
	nodes map[K]*lruNode[K]
	head  *lruNode[K] // dummy head (most recently used)
	tail  *lruNode[K] // dummy tail (least recently used)
}

// NewLRU creates a new LRU policy instance.
func NewLRU[K comparable]() *LRU[K] {
	l := &LRU[K]{
		nodes: make(map[K]*lruNode[K]),
		head:  &lruNode[K]{},
		tail:  &lruNode[K]{},
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}

func (l *LRU[K]) insertAtHead(node *lruNode[K]) {
	node.next = l.head.next
	node.prev = l.head
	l.head.next.prev = node
	l.head.next = node
}

func (l *LRU[K]) removeNode(node *lruNode[K]) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
}

func (l *LRU[K]) OnAccess(key K) {
	if node, exists := l.nodes[key]; exists {
		l.removeNode(node)
		l.insertAtHead(node)
	}
}

func (l *LRU[K]) OnInsert(key K) {
	if node, exists := l.nodes[key]; exists {
		l.removeNode(node)
		l.insertAtHead(node)
		return
	}
	node := &lruNode[K]{key: key}
	l.nodes[key] = node
	l.insertAtHead(node)
}

func (l *LRU[K]) OnRemove(key K) {
	if node, exists := l.nodes[key]; exists {
		l.removeNode(node)
		delete(l.nodes, key)
	}
}

func (l *LRU[K]) Evict() (K, bool) {
	var zero K
	if l.head.next == l.tail {
		return zero, false
	}
	// Least recently used is at the tail
	node := l.tail.prev
	l.removeNode(node)
	delete(l.nodes, node.key)
	return node.key, true
}

func (l *LRU[K]) Len() int {
	return len(l.nodes)
}

func (l *LRU[K]) Clear() {
	l.nodes = make(map[K]*lruNode[K])
	l.head.next = l.tail
	l.tail.prev = l.head
}
