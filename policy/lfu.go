package policy

type lfuNode[K comparable] struct {
	key       K
	freqBlock *freqBlock[K]
	prev      *lfuNode[K]
	next      *lfuNode[K]
}

type freqBlock[K comparable] struct {
	freq int
	prev *freqBlock[K]
	next *freqBlock[K]
	head *lfuNode[K] // dummy head for items with this frequency (newest accessed)
	tail *lfuNode[K] // dummy tail for items with this frequency (oldest accessed)
}

func newFreqBlock[K comparable](freq int) *freqBlock[K] {
	fb := &freqBlock[K]{
		freq: freq,
		head: &lfuNode[K]{},
		tail: &lfuNode[K]{},
	}
	fb.head.next = fb.tail
	fb.tail.prev = fb.head
	return fb
}

func (fb *freqBlock[K]) isEmpty() bool {
	return fb.head.next == fb.tail
}

func (fb *freqBlock[K]) insertNode(node *lfuNode[K]) {
	node.freqBlock = fb
	node.next = fb.head.next
	node.prev = fb.head
	fb.head.next.prev = node
	fb.head.next = node
}

func (fb *freqBlock[K]) removeNode(node *lfuNode[K]) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
	node.freqBlock = nil
}

// LFU implements the Least Frequently Used eviction policy in O(1) time.
type LFU[K comparable] struct {
	nodes    map[K]*lfuNode[K]
	freqHead *freqBlock[K] // dummy head for frequency blocks list (lowest frequencies)
	freqTail *freqBlock[K] // dummy tail for frequency blocks list (highest frequencies)
}

// NewLFU creates a new LFU policy instance.
func NewLFU[K comparable]() *LFU[K] {
	l := &LFU[K]{
		nodes:    make(map[K]*lfuNode[K]),
		freqHead: &freqBlock[K]{freq: 0},
		freqTail: &freqBlock[K]{freq: 0},
	}
	l.freqHead.next = l.freqTail
	l.freqTail.prev = l.freqHead
	return l
}

func (l *LFU[K]) insertFreqBlockAfter(newBlock, target *freqBlock[K]) {
	newBlock.next = target.next
	newBlock.prev = target
	target.next.prev = newBlock
	target.next = newBlock
}

func (l *LFU[K]) removeFreqBlock(fb *freqBlock[K]) {
	fb.prev.next = fb.next
	fb.next.prev = fb.prev
	fb.prev = nil
	fb.next = nil
}

func (l *LFU[K]) incrementFreq(node *lfuNode[K]) {
	currentBlock := node.freqBlock
	nextBlock := currentBlock.next

	if nextBlock == l.freqTail || nextBlock.freq != currentBlock.freq+1 {
		newBlock := newFreqBlock[K](currentBlock.freq + 1)
		l.insertFreqBlockAfter(newBlock, currentBlock)
		nextBlock = newBlock
	}

	currentBlock.removeNode(node)
	nextBlock.insertNode(node)

	if currentBlock.isEmpty() {
		l.removeFreqBlock(currentBlock)
	}
}

func (l *LFU[K]) OnAccess(key K) {
	if node, exists := l.nodes[key]; exists {
		l.incrementFreq(node)
	}
}

func (l *LFU[K]) OnInsert(key K) {
	if node, exists := l.nodes[key]; exists {
		l.incrementFreq(node)
		return
	}

	node := &lfuNode[K]{key: key}
	l.nodes[key] = node

	// Initial frequency is 1. Check if we have block with frequency 1.
	firstBlock := l.freqHead.next
	if firstBlock == l.freqTail || firstBlock.freq != 1 {
		newBlock := newFreqBlock[K](1)
		l.insertFreqBlockAfter(newBlock, l.freqHead)
		firstBlock = newBlock
	}
	firstBlock.insertNode(node)
}

func (l *LFU[K]) OnRemove(key K) {
	if node, exists := l.nodes[key]; exists {
		fb := node.freqBlock
		fb.removeNode(node)
		delete(l.nodes, key)
		if fb.isEmpty() {
			l.removeFreqBlock(fb)
		}
	}
}

func (l *LFU[K]) Evict() (K, bool) {
	var zero K
	firstBlock := l.freqHead.next
	if firstBlock == l.freqTail {
		return zero, false
	}
	// Evict the least recently used node in the lowest frequency block
	node := firstBlock.tail.prev
	firstBlock.removeNode(node)
	delete(l.nodes, node.key)

	if firstBlock.isEmpty() {
		l.removeFreqBlock(firstBlock)
	}
	return node.key, true
}

func (l *LFU[K]) Len() int {
	return len(l.nodes)
}

func (l *LFU[K]) Clear() {
	l.nodes = make(map[K]*lfuNode[K])
	l.freqHead.next = l.freqTail
	l.freqTail.prev = l.freqHead
}
