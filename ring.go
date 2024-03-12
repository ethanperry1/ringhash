// Package ring provides all functionality related to ring hashing and key
// distribution across multiple nodes. It contains the definition for Ring which is a full
// implementation of the ring hash data structure with an additional notification mechanism that
// leverages channels. Additionally, it provides a definition for a broadcasting ring, which is a
// data structure implementing the same KeyNodeWatcher interface used by the Ring,
// with the primary difference that every node always receives every hash/key in the broadcasting ring,
// whereas only a single node receives a hash/key at a time with the ordinary Ring.
// Lastly, it provides hashing algorithms which can be used for hashing keys onto the ring, although custom
// algorithms can also be used.
package ring

import (
	"fmt"
	"sort"
	"sync"
)

type State struct {
	NodesBySlice map[uint64]string `json:"nodesBySlice"`
	SlicesByHash map[uint64]uint64 `json:"slicesByHash"`
	HashesByKey  map[string]uint64 `json:"hashesByKey"`
}

// Op is a struct describing the movement of a key-value pair of the ring changing --
// either moving from one slice of the ring to another, being added to the ring, or being removed.
type Op[T any] struct {
	Key        string
	Node       string
	Payload    T
	Removed    bool
	Updated    bool
	RingChange bool
}

// Node is the struct describing a single node of the hash ring, with its corresponding
// identifier used for hashing and VFactor for creating virtual slices of the node.
type Node struct {
	Identifier string
	VFactor    int
}

// InnerKey is a struct describing a single, unique key in the system.
// The key has an associated order property which specifies the order in which notifications
// occur with respect to the other keys that have hashed to the same position in the ring.
// A lower order implies that this key will appear in a change notification before the
// other keys which hash to the same position on the ring.
type InnerKey struct {
	Key   string
	Order int
}

type Key[T any] struct {
	*InnerKey
	Value T
}

// Watcher is an interface whose implementation should register and deregister channels which can watch
// for changes in a hash ring depending on the filter condition of the hash ring.
type Watcher[T any] interface {
	RegisterWatcher(filter Op[T]) chan Op[T]
	DeregisterWatcher(op Op[T])
}

// Keys is an interface whose implementation should add and remove keys from a collection,
// using an optional hash key to perform the hashing operation involved in emplacing the key in the collection.
type Keys[T any] interface {
	Emplace(key *Key[T], hk ...string) error
	Update(key *Key[T]) error
	Remove(string)
}

// Nodes is an interface whose implementation should create, update, delete, get, and list
// a collection of nodes, throwing errors if necessary.
type Nodes interface {
	CreateNode(node Node) error
	DeleteNode(identifier string)
	UpdateNode(node Node) error
	GetNode(identifier string) (Node, error)
	ListNodes() []string
}

// KeyNodeWatcher is a composite of the three core interfaces of the hash ring.
// It encompasses all essential functionality of the hash ring.
type KeyNodeWatcher[T any] interface {
	Keys[T]
	Nodes
	Watcher[T]
	State() *State
}

type opChans[T any] struct {
	msg  chan Op[T]
	done chan struct{}
	wg   *sync.WaitGroup
}

type watcher[T any] struct {
	watchMu  sync.Mutex
	watchers map[string]opChans[T]
	Filter   func(Op[T]) string
}

// RegisterWatcher provides a channel of Ops for any key-value changes of an inserted node.
// If the node registered does not exist, no notifications will come through until that node
// is inserted into the ring.
func (ring *watcher[T]) RegisterWatcher(filter Op[T]) chan Op[T] {
	ring.watchMu.Lock()
	defer ring.watchMu.Unlock()
	opChans := opChans[T]{
		msg:  make(chan Op[T]),
		done: make(chan struct{}),
		wg:   new(sync.WaitGroup),
	}
	ring.watchers[ring.Filter(filter)] = opChans
	return opChans.msg
}

// DeregisterWatcher attempts to close the channel and delete the registration from memory.
// It is a noop if the watcher does not exist.
func (ring *watcher[T]) DeregisterWatcher(op Op[T]) {
	ring.watchMu.Lock()

	filter := ring.Filter(op)
	c, ok := ring.watchers[filter]
	if !ok {
		ring.watchMu.Unlock()
		return
	}
	delete(ring.watchers, filter)
	ring.watchMu.Unlock()

	close(c.done)
	c.wg.Wait()
	close(c.msg)
}

func (ring *watcher[T]) notify(op Op[T]) {
	ring.watchMu.Lock()
	watcher, ok := ring.watchers[ring.Filter(op)]
	if !ok {
		ring.watchMu.Unlock()
		return
	}
	watcher.wg.Add(1)
	defer watcher.wg.Done()
	ring.watchMu.Unlock()

	select {
	case watcher.msg <- op:
	case <-watcher.done:
	}
}

// Ring is a hash ring implementation capable of storing key value pairs belonging to member
// nodes in one or more slices belonging to these nodes. The ring can be observed for changes
// of the key value pairs (removal, addition, slice changes).
type Ring[T any] struct {
	slices []uint64
	hashes []uint64
	empty  map[uint64]uint64

	nodesBySlice  map[uint64]string
	vFactorByNode map[string]int
	slicesByHash  map[uint64]uint64
	keysByHash    map[uint64][]*InnerKey
	contentByKey  map[string]T
	hashesByKey   map[string]uint64
	mu            sync.RWMutex

	Hash        func(string) uint64
	BaseVFactor int
	ToSliceName func(string, int) string

	watcher[T]
}

// New attempts to create a new ring, given an optional function to modify public fields of the ring.
func New[T any](options ...func(*Ring[T])) (*Ring[T], error) {
	ring := &Ring[T]{
		nodesBySlice:  make(map[uint64]string),
		vFactorByNode: make(map[string]int),
		slicesByHash:  make(map[uint64]uint64),
		keysByHash:    make(map[uint64][]*InnerKey),
		hashesByKey:   make(map[string]uint64),
		contentByKey:  make(map[string]T),
		empty:         make(map[uint64]uint64),
		Hash:          MD5,
		BaseVFactor:   1,
		ToSliceName: func(s string, i int) string {
			return fmt.Sprintf("%s%d", s, i)
		},
		watcher: watcher[T]{
			watchers: make(map[string]opChans[T]),
			Filter: func(o Op[T]) string {
				return o.Node
			},
		},
	}

	for _, option := range options {
		option(ring)
	}

	// Throw error if base factor is less than 1.
	if ring.BaseVFactor < 1 {
		return nil, ErrInvalidBaseVFactor
	}

	return ring, nil
}

func (ring *Ring[T]) State() *State {
	return &State{
		NodesBySlice: ring.nodesBySlice,
		SlicesByHash: ring.slicesByHash,
		HashesByKey:  ring.hashesByKey,
	}
}

// CreateNode attempts to add a new node to the hash ring, including all of that nodes associated slices.
// The nodes VFactor determines how many slices will be associated with the particular node.
func (ring *Ring[T]) CreateNode(node Node) error {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	// Check to see if node already exists.
	_, ok := ring.vFactorByNode[node.Identifier]
	if ok {
		return ErrNodeAlreadyExists
	}

	// Save vfactor.
	ring.vFactorByNode[node.Identifier] = node.VFactor

	// Create all virtual slices.
	for idx := 0; idx < node.VFactor*ring.BaseVFactor; idx++ {

		// Compute slice hash and insert slice.
		slice := ring.Hash(ring.ToSliceName(node.Identifier, idx))

		err := ring.insertSlice(slice, node.Identifier)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteNode attempts to remove a node from the hash ring given the node's identifier.
// It is a noop if no node with the given identifier exists.
func (ring *Ring[T]) DeleteNode(identifier string) {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	// Check if the node exists.
	vFactor, ok := ring.vFactorByNode[identifier]
	if !ok {
		return
	}

	for idx := 0; idx < vFactor*ring.BaseVFactor; idx++ {
		ring.removeSlice(ring.Hash(ring.ToSliceName(identifier, idx)))
	}

	// Delete vFactor.
	delete(ring.vFactorByNode, identifier)
}

// UpdateNode attempts to update a node by adding or removing slices based on the new VFactor of that node.
// If the VFactor is the same as it was previously, nothing will change.
func (ring *Ring[T]) UpdateNode(node Node) error {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	vFactor, ok := ring.vFactorByNode[node.Identifier]
	if !ok {
		return ErrNodeNotFound
	}

	if node.VFactor == vFactor {
		return nil
	}

	if node.VFactor > vFactor {
		for idx := vFactor * ring.BaseVFactor; idx < node.VFactor*ring.BaseVFactor; idx++ {
			slice := ring.Hash(ring.ToSliceName(node.Identifier, idx))
			err := ring.insertSlice(slice, node.Identifier)
			if err == ErrSliceAlreadyExists {
				return ErrSliceHashCollision
			}
		}
	} else {
		for idx := node.VFactor * ring.BaseVFactor; idx < vFactor*ring.BaseVFactor; idx++ {
			slice := ring.Hash(ring.ToSliceName(node.Identifier, idx))
			ring.removeSlice(slice)
		}
	}

	ring.vFactorByNode[node.Identifier] = node.VFactor

	return nil
}

// GetNode attempts to find the node with the provided identifier.
func (ring *Ring[T]) GetNode(identifier string) (Node, error) {
	ring.mu.RLock()
	defer ring.mu.RUnlock()
	vFactor, ok := ring.vFactorByNode[identifier]
	if !ok {
		return Node{}, ErrNodeNotFound
	}

	return Node{
		Identifier: identifier,
		VFactor:    vFactor,
	}, nil
}

// ListNodes lists the identifiers of the current nodes of the hash ring.
func (ring *Ring[T]) ListNodes() []string {
	ring.mu.RLock()
	defer ring.mu.RUnlock()
	nodes := make([]string, 0, len(ring.vFactorByNode))

	for node := range ring.vFactorByNode {
		nodes = append(nodes, node)
	}

	return nodes
}

func (ring *Ring[T]) insertSlice(slice uint64, node string) error {

	// Check to see if slice already exists.
	_, ok := ring.nodesBySlice[slice]
	if ok {
		return ErrSliceAlreadyExists
	}

	// Insert new slice into slices and retrieve index.
	var idx int
	ring.slices, idx = insertPreserveOrder(ring.slices, slice, findIndex)

	// Add to nodes by slice.
	ring.nodesBySlice[slice] = node

	// If this is the first slice, attempt to move in keys from the empty container.
	if len(ring.slices) == 1 {
		for _, hash := range ring.empty {
			ring.slicesByHash[hash] = slice
			for _, key := range ring.keysByHash[hash] {
				ring.notify(Op[T]{
					Key:        key.Key,
					Payload:    ring.contentByKey[key.Key],
					Node:       ring.nodesBySlice[slice],
					RingChange: true,
				})
			}
			delete(ring.empty, hash)
		}
	} else { // Otherwise convert the hashes taken from the next slice.
		nextSlice := ring.slices[findNextIndex(ring.slices, idx)]

		// Convert ring hashes to new slice starting at the first hash greater than the new slice and ending at the next slice.
		ring.convertHashes(
			slice,
			findIndex(ring.hashes, slice),
			findIndex(ring.hashes, nextSlice),
			nextSlice < slice,
		)
	}

	return nil
}

func (ring *Ring[T]) removeSlice(slice uint64) {

	// Noop if slice doesn't exist.
	_, ok := ring.nodesBySlice[slice]
	if !ok {
		return
	}

	// Find the current index of the slice.
	sliceIdx := findIndex(ring.slices, slice)

	// If this is the final slice in the ring, move hashes into the empty container.
	if len(ring.slices) == 1 {
		for _, hash := range ring.hashes {
			for _, key := range ring.keysByHash[hash] {
				ring.notify(Op[T]{
					Key:        key.Key,
					Payload:    ring.contentByKey[key.Key],
					Node:       ring.nodesBySlice[slice],
					Removed:    true,
					RingChange: true,
				})
			}
			ring.empty[hash] = hash
		}
	} else {
		// Find the previous slice.
		prevSlice := ring.slices[findPrevIndex(ring.slices, sliceIdx)]

		nextSlice := ring.slices[findNextIndex(ring.slices, sliceIdx)]

		// Convert all of this slice's hashes to belong to the previous slice.
		ring.convertHashes(
			prevSlice,
			findIndex(ring.hashes, slice),
			findIndex(ring.hashes, nextSlice),
			nextSlice < slice,
		)
	}

	// Remove the slice from the slices array.
	ring.slices, _ = removeIndex(ring.slices, sliceIdx)

	// Delete from nodes by slice map.
	delete(ring.nodesBySlice, slice)
}

func (ring *Ring[T]) convertHashes(
	slice uint64,
	hashStartIndex int,
	hashEndIndex int,
	circle bool,
) {

	// If start and end are equal, convert the entire ring.
	if circle && hashStartIndex == hashEndIndex ||
		hashStartIndex == 0 && hashEndIndex == len(ring.hashes) {
		for _, hash := range ring.hashes {
			ring.convertHash(slice, hash)
		}
		return
	}

	if hashEndIndex == len(ring.hashes) {
		hashEndIndex = 0
	}

	// Otherwise convert only the specified range.
	for {
		if hashStartIndex == len(ring.hashes) {
			hashStartIndex = 0
		}

		if hashStartIndex == hashEndIndex {
			return
		}

		ring.convertHash(slice, ring.hashes[hashStartIndex])

		hashStartIndex++
	}

}

func (ring *Ring[T]) convertHash(slice uint64, hash uint64) {

	// Notify previous node of removals.
	prevSlice := ring.slicesByHash[hash]
	for _, key := range ring.keysByHash[hash] {
		ring.notify(Op[T]{
			Key:        key.Key,
			Payload:    ring.contentByKey[key.Key],
			Node:       ring.nodesBySlice[prevSlice],
			Removed:    true,
			RingChange: true,
		})
	}

	// Reassign hash's slice and notify addition.
	ring.slicesByHash[hash] = slice
	for _, key := range ring.keysByHash[hash] {
		ring.notify(Op[T]{
			Key:        key.Key,
			Payload:    ring.contentByKey[key.Key],
			Node:       ring.nodesBySlice[slice],
			RingChange: true,
		})
	}
}

// Emplace attempts to add the given key to the hash ring.
// If the optional hash key is provided, this will be used to hash the key into the ring.
// Otherwise, the key itself will be used to hash into the ring.
// The key must unique; an error will be thrown otherwise.
func (ring *Ring[T]) Emplace(key *Key[T], hk ...string) error {
	if key == nil {
		return ErrNilKey
	}

	ring.mu.Lock()
	defer ring.mu.Unlock()

	// Check to see if key already exists.
	_, ok := ring.hashesByKey[key.InnerKey.Key]
	if ok {
		return ErrKeyAlreadyExists
	}

	// Insert key content into keysByKey map.
	ring.contentByKey[key.InnerKey.Key] = key.Value

	// Identify which key will be used to create the hash.
	var hashKey string
	if len(hk) == 0 {
		hashKey = key.InnerKey.Key
	} else {
		hashKey = hk[0]
	}

	// Hash the key.
	hash := ring.Hash(hashKey)

	// Insert into hash ring.
	ring.insertHash(hash)

	// Check to see if there are any slices to take the key.
	if len(ring.slices) == 0 {
		ring.empty[hash] = hash

		ring.notify(Op[T]{
			Key:     key.InnerKey.Key,
			Payload: key.Value,
		})
	} else {
		// Find the appropriate slice this hash will belong to.
		slice := ring.slices[findPrevIndex(ring.slices, findIndex(ring.slices, hash))]
		ring.slicesByHash[hash] = slice

		ring.notify(Op[T]{
			Key:     key.InnerKey.Key,
			Node:    ring.nodesBySlice[slice],
			Payload: key.Value,
		})
	}

	// Insert key into keys array for this hash.
	ring.keysByHash[hash], _ = insertPreserveOrder(
		ring.keysByHash[hash],
		key.InnerKey,
		findKeyIndex,
	)

	// Insert key into hashes by key table.
	ring.hashesByKey[key.InnerKey.Key] = hash

	return nil
}

// Update attempts to update the key object in the ring without changing
// its position in the ring, or its hash.
func (ring *Ring[T]) Update(key *Key[T]) error {
	if key == nil {
		return ErrNilKey
	}

	// Assure key is actually present in ring.
	_, ok := ring.contentByKey[key.InnerKey.Key]
	if !ok {
		return ErrKeyNotFound
	}

	// Update key in keysByKey map.
	ring.contentByKey[key.InnerKey.Key] = key.Value

	// Notify subscribers of key update.
	ring.notify(Op[T]{
		Key:     key.InnerKey.Key,
		Payload: key.Value,
		Node:    ring.nodesBySlice[ring.slicesByHash[ring.hashesByKey[key.InnerKey.Key]]],
		Updated: true,
	})

	return nil
}

// Remove will remove a key from the ring, given its unique key.
func (ring *Ring[T]) Remove(key string) {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	// Noop if the key doesn't exist.
	hash, ok := ring.hashesByKey[key]
	if !ok {
		return
	}

	// Delete from keysByKey map.
	delete(ring.contentByKey, key)

	// Remove the key from the keys by hash table for this hash.
	ring.keysByHash[hash], _ = removeIndex(
		ring.keysByHash[hash],
		findKeyByName(ring.keysByHash[hash], key),
	)

	// If the empty container has any elements, remove from the empty container.
	if len(ring.empty) > 0 {
		delete(ring.empty, hash)

		ring.notify(Op[T]{
			Key:     key,
			Removed: true,
		})
	} else {

		// Notify new key removal from ring.
		ring.notify(Op[T]{
			Key:     key,
			Node:    ring.nodesBySlice[ring.slicesByHash[hash]],
			Removed: true,
		})
	}

	// If this was the last key left for this hash, remove the hash.
	if len(ring.keysByHash[hash]) == 0 {
		// Remove the hash.
		ring.removeHash(hash)

		// Remove from slices by hash table.
		delete(ring.slicesByHash, hash)
	}

	// Delete key from hashes by key table/
	delete(ring.hashesByKey, key)
}

func (ring *Ring[T]) insertHash(hash uint64) {
	idx := findIndex(ring.hashes, hash)
	if idx >= len(ring.hashes) || ring.hashes[idx] != hash {
		ring.hashes, _ = insertPreserveOrder(ring.hashes, hash, findIndex)
	}
}

func (ring *Ring[T]) removeHash(hash uint64) {
	idx := findIndex(ring.hashes, hash)
	if idx < len(ring.hashes) && ring.hashes[idx] == hash {
		ring.hashes, _ = removeIndex(ring.hashes, idx)
	}
}

func findKeyIndex(t []*InnerKey, k *InnerKey) int {
	return sort.Search(
		len(t),
		func(i int) bool { return t[i].Order >= k.Order },
	)
}

func findKeyByName(keys []*InnerKey, name string) int {
	for idx, key := range keys {
		if key.Key == name {
			return idx
		}
	}

	return -1
}

// findIndex will return the index where val is located, or should be inserted (if it is not located in the array).
func findIndex(arr []uint64, val uint64) int {
	return sort.Search(len(arr), func(i int) bool { return arr[i] >= val })
}

// findPrevIndex will return the index previous to the current index.
func findPrevIndex(arr []uint64, idx int) int {
	if idx == 0 {
		return len(arr) - 1
	}

	return idx - 1
}

func findNextIndex(arr []uint64, idx int) int {
	if idx == len(arr)-1 {
		return 0
	}

	return idx + 1
}

func insertPreserveOrder[T any](
	arr []T,
	val T,
	findIndex func([]T, T) int,
) ([]T, int) {
	idx := findIndex(arr, val)
	if idx == len(arr) {
		arr = append(arr, val)
	} else {
		arr = append(arr[:idx+1], arr[idx:]...)
		arr[idx] = val
	}
	return arr, idx
}

func removeIndex[T any](arr []T, idx int) ([]T, error) {
	if len(arr) == 0 {
		return nil, nil
	}
	if idx < 0 || idx >= len(arr) {
		return arr, ErrOutOfBounds
	}
	return append(arr[:idx], arr[idx+1:]...), nil
}
