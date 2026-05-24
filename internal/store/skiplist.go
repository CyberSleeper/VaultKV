package store

import (
	"math/rand/v2"
)

const (
	// probability for each level increment on the new node
	probability = 0.25
	maxLevel    = 16
)

type Node struct {
	Key   string
	Value string
	Next  []*Node
	level int
}

type Skiplist struct {
	SizeBytes int
	BeginNode *Node
}

func NewNode() *Node {
	return &Node{
		Next: make([]*Node, maxLevel+1),
	}
}

func NewSkiplist() *Skiplist {
	beginNode := NewNode()

	return &Skiplist{
		BeginNode: beginNode,
	}
}

func randomLevel() int {
	lvl := 0
	for rand.Float32() < probability && lvl < maxLevel {
		lvl++
	}
	return lvl
}

func (s *Skiplist) insert(befNode [maxLevel + 1]*Node, k, v string) {
	curNode := NewNode()
	curNode.Key = k
	curNode.Value = v
	curNode.level = randomLevel()

	for i := range curNode.level + 1 {
		curNode.Next[i] = befNode[i].Next[i]
		befNode[i].Next[i] = curNode
	}
}

func (s *Skiplist) getUpdatePath(k string) [maxLevel + 1]*Node {
	// Store the last visited node for each level which key is
	// STRICTLY less than k
	var lastNodes [maxLevel + 1]*Node
	curNode := s.BeginNode

	for i := maxLevel; i >= 0; i-- {
		for curNode.Next[i] != nil && curNode.Next[i].Key < k {
			curNode = curNode.Next[i]
		}
		lastNodes[i] = curNode
	}
	return lastNodes
}

func (s *Skiplist) Set(k, v string) {
	lastNodes := s.getUpdatePath(k)
	candidate := lastNodes[0].Next[0]

	if candidate != nil && candidate.Key == k {
		s.SizeBytes += len(v) - len(candidate.Value)
		candidate.Value = v
	} else {
		s.SizeBytes += len(k) + len(v)
		s.insert(lastNodes, k, v)
	}
}

func (s *Skiplist) Get(k string) (string, bool) {
	lastNodes := s.getUpdatePath(k)
	candidate := lastNodes[0].Next[0]

	if candidate != nil && candidate.Key == k {
		return candidate.Value, true
	}
	return "", false
}

func (s *Skiplist) IsFull() bool {
	return s.SizeBytes >= memTableSizeThreshold
}
