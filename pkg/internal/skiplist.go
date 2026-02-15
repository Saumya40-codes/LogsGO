package internal

import (
	"math/rand"
)

type Value struct {
	Service string
	Level   string
	Message string
}

type Node struct {
	key     int64   // timestamp
	values  []Value // list of log keys at this timestamp
	forward []*Node // per level next pointers
}

type SkipList struct {
	level    int
	head     *Node
	maxLevel int
}

type IteratorSearchOpts struct {
	Start int64
	End   int64
}

type Iterator struct {
	curr *Node
	end  int64
}

func NewSkipList() *SkipList {
	maxLevel := 8
	return &SkipList{
		level: 0,
		head: &Node{ // sentinel
			key:     -1,
			forward: make([]*Node, maxLevel),
		},
		maxLevel: maxLevel, // shouhld it be configurable?
	}
}

func (s *SkipList) Insert(key int64, value Value) {
	curr := s.head
	predecessors := make([]*Node, s.maxLevel)

	// get all predecessors
	for level := s.level; level >= 0; level-- {
		for curr.forward[level] != nil && curr.forward[level].key < key {
			curr = curr.forward[level]
		}
		predecessors[level] = curr
	}

	// merge logs at same timestamp at L0 if same ts already exist and early return from here
	next := predecessors[0].forward[0]
	if next != nil && next.key == key {
		next.values = append(next.values, value)
		return
	}

	// determine max promotion level
	lvl := 0
	for shouldPromoteToNextLevel() && lvl < s.maxLevel-1 {
		lvl++
	}
	if lvl > s.level {
		for i := s.level + 1; i <= lvl; i++ {
			predecessors[i] = s.head
		}
		s.level = lvl
	}

	// create new node
	newNode := &Node{
		key:     key,
		values:  []Value{value},
		forward: make([]*Node, lvl+1),
	}

	// for each level, rewire the pointers
	for level := lvl; level >= 0; level-- {
		newNode.forward[level] = predecessors[level].forward[level]
		predecessors[level].forward[level] = newNode
	}
}

func (s *SkipList) Delete(key int64) {
	curr := s.head
	update := make([]*Node, s.maxLevel)

	for level := s.level; level >= 0; level-- {
		for curr.forward[level] != nil && curr.forward[level].key < key {
			curr = curr.forward[level]
		}
		update[level] = curr
	}

	target := curr.forward[0]
	if target == nil || target.key != key {
		return
	}

	for level := 0; level <= s.level; level++ {
		if update[level].forward[level] != target {
			break // target not found at this level so it won't have been promoted to subsequent levels
		}
		update[level].forward[level] = target.forward[level]
	}

	for s.level > 0 && s.head.forward[s.level] == nil {
		s.level--
	}
}

func shouldPromoteToNextLevel() bool {
	return rand.Float64() < 0.5
}

// Floor returns max key which is smaller than or equal to the given key
func (s *SkipList) Floor(key int64) (*Node, bool) {
	curr := s.head
	for level := s.level; level >= 0; level-- {
		for curr.forward[level] != nil && curr.forward[level].key <= key {
			curr = curr.forward[level]
		}
	}

	if curr != s.head {
		return curr, true
	}

	return nil, false
}

func (s *SkipList) Seek(opts IteratorSearchOpts) *Iterator {
	// forward seek = first >= start
	curr := s.head
	for level := s.level; level >= 0; level-- {
		for curr.forward[level] != nil && curr.forward[level].key < opts.Start {
			curr = curr.forward[level]
		}
	}
	curr = curr.forward[0]

	return &Iterator{
		curr: curr,
		end:  opts.End,
	}
}

func (it *Iterator) Next() (*Node, bool) {
	if it.curr == nil {
		return nil, false
	}

	if it.end != 0 && it.curr.key > it.end {
		return nil, false
	}

	node := it.curr
	it.curr = it.curr.forward[0]
	return node, true
}

func (n *Node) GetKey() int64 {
	return n.key
}

func (n *Node) GetValues() []Value {
	return n.values
}
