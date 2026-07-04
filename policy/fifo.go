package policy

type fifoNode[K comparable] struct {
	key  K
	prev *fifoNode[K]
	next *fifoNode[K]
}

// FIFO implements the First In First Out eviction policy.
type FIFO[K comparable] struct {
	nodes map[K]*fifoNode[K]
	head  *fifoNode[K] // dummy head (newest)
	tail  *fifoNode[K] // dummy tail (oldest)
}

// NewFIFO creates a new FIFO policy instance.
func NewFIFO[K comparable]() *FIFO[K] {
	f := &FIFO[K]{
		nodes: make(map[K]*fifoNode[K]),
		head:  &fifoNode[K]{},
		tail:  &fifoNode[K]{},
	}
	f.head.next = f.tail
	f.tail.prev = f.head
	return f
}

func (f *FIFO[K]) insertAtHead(node *fifoNode[K]) {
	node.next = f.head.next
	node.prev = f.head
	f.head.next.prev = node
	f.head.next = node
}

func (f *FIFO[K]) removeNode(node *fifoNode[K]) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
}

func (f *FIFO[K]) OnAccess(key K) {
	// FIFO does not modify order on read/access
}

func (f *FIFO[K]) OnInsert(key K) {
	if _, exists := f.nodes[key]; exists {
		// Existing keys keep their FIFO position when re-written
		return
	}
	node := &fifoNode[K]{key: key}
	f.nodes[key] = node
	f.insertAtHead(node)
}

func (f *FIFO[K]) OnRemove(key K) {
	if node, exists := f.nodes[key]; exists {
		f.removeNode(node)
		delete(f.nodes, key)
	}
}

func (f *FIFO[K]) Evict() (K, bool) {
	var zero K
	if f.head.next == f.tail {
		return zero, false
	}
	// Oldest is at the tail
	node := f.tail.prev
	f.removeNode(node)
	delete(f.nodes, node.key)
	return node.key, true
}

func (f *FIFO[K]) Len() int {
	return len(f.nodes)
}

func (f *FIFO[K]) Clear() {
	f.nodes = make(map[K]*fifoNode[K])
	f.head.next = f.tail
	f.tail.prev = f.head
}
