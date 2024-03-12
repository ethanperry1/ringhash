package ring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	m.Run()
}

type RingPayloadType struct{}

func TestInsertHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	ring.insertHash(1)
	ring.insertHash(5)
	ring.insertHash(2)
	ring.insertHash(1)

	require.Equal(t, []uint64{1, 2, 5}, ring.hashes)
}

func TestEmptyRemoveHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	ring.removeHash(1)

	require.Equal(t, 0, len(ring.hashes))
}

func TestInsertAndRemoveHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	ring.insertHash(1)
	ring.insertHash(5)
	ring.insertHash(2)
	ring.insertHash(1)

	require.Equal(t, []uint64{1, 2, 5}, ring.hashes)

	ring.removeHash(6)
	ring.removeHash(7)
	ring.removeHash(2)

	require.Equal(t, []uint64{1, 5}, ring.hashes)

	ring.removeHash(5)
	ring.removeHash(1)

	require.Equal(t, []uint64{}, ring.hashes)
}

func TestBasicConvertHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4, 7, 9}
	ring.slicesByHash[1] = 1
	ring.slicesByHash[2] = 1
	ring.slicesByHash[3] = 3
	ring.slicesByHash[4] = 3
	ring.slicesByHash[7] = 3
	ring.slicesByHash[9] = 3

	ring.convertHashes(5, 4, 0, false)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 1,
		3: 3,
		4: 3,
		7: 5,
		9: 5,
	}, ring.slicesByHash)

	ring.convertHashes(2, 1, 2, false)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 2,
		3: 3,
		4: 3,
		7: 5,
		9: 5,
	}, ring.slicesByHash)

	ring.convertHashes(8, 5, 0, false)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 2,
		3: 3,
		4: 3,
		7: 5,
		9: 8,
	}, ring.slicesByHash)

	ring.convertHashes(5, 5, 0, false)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 2,
		3: 3,
		4: 3,
		7: 5,
		9: 5,
	}, ring.slicesByHash)
}

func TestEdgeCaseOne(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(6, "A")
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4}
	ring.slicesByHash[1] = 6
	ring.slicesByHash[2] = 6
	ring.slicesByHash[3] = 6
	ring.slicesByHash[4] = 6

	err = ring.insertSlice(5, "A")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		1: 6,
		2: 6,
		3: 6,
		4: 6,
	}, ring.slicesByHash)
}

func TestEdgeCaseTwo(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(5, "A")
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4}
	ring.slicesByHash[1] = 5
	ring.slicesByHash[2] = 5
	ring.slicesByHash[3] = 5
	ring.slicesByHash[4] = 5

	err = ring.insertSlice(6, "A")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		1: 6,
		2: 6,
		3: 6,
		4: 6,
	}, ring.slicesByHash)
}

func TestEdgeCaseThree(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(1, "A")
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4}
	ring.slicesByHash[1] = 1
	ring.slicesByHash[2] = 1
	ring.slicesByHash[3] = 1
	ring.slicesByHash[4] = 1

	err = ring.insertSlice(6, "A")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 1,
		3: 1,
		4: 1,
	}, ring.slicesByHash)
}

func TestEdgeCaseFour(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(1, "A")
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4}
	ring.slicesByHash[1] = 1
	ring.slicesByHash[2] = 1
	ring.slicesByHash[3] = 1
	ring.slicesByHash[4] = 1

	err = ring.insertSlice(0, "A")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 1,
		3: 1,
		4: 1,
	}, ring.slicesByHash)
}

func TestEdgeCaseFive(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(0, "A")
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4}
	ring.slicesByHash[1] = 0
	ring.slicesByHash[2] = 0
	ring.slicesByHash[3] = 0
	ring.slicesByHash[4] = 0

	err = ring.insertSlice(1, "A")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 1,
		3: 1,
		4: 1,
	}, ring.slicesByHash)
}

func TestEdgeCaseSix(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(6, "A")
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 3, 4}
	ring.slicesByHash[1] = 6
	ring.slicesByHash[2] = 6
	ring.slicesByHash[3] = 6
	ring.slicesByHash[4] = 6

	err = ring.insertSlice(1, "A")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		1: 1,
		2: 1,
		3: 1,
		4: 1,
	}, ring.slicesByHash)
}

func TestEmptyConvertHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	expectedResult := map[uint64]uint64{}

	ring.hashes = []uint64{}

	ring.convertHashes(0, 0, 0, false)

	require.Equal(t, expectedResult, ring.slicesByHash)
}

func TestSingleConvertHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	ring.hashes = []uint64{1}
	ring.slicesByHash[1] = 0

	ring.convertHashes(2, 0, 0, false)

	require.Equal(t, map[uint64]uint64{1: 0}, ring.slicesByHash)
}

func TestSingleConvertHashesWithCircling(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	ring.hashes = []uint64{0}
	ring.slicesByHash[0] = 1

	ring.convertHashes(2, 0, 0, true)

	require.Equal(t, map[uint64]uint64{0: 2}, ring.slicesByHash)
}

func TestHomogeneousConvertHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	ring.hashes = []uint64{1, 2, 5, 6}
	ring.slicesByHash[1] = 2
	ring.slicesByHash[2] = 2
	ring.slicesByHash[5] = 2
	ring.slicesByHash[6] = 2

	ring.convertHashes(3, 2, 1, false)

	require.Equal(t, map[uint64]uint64{
		1: 3,
		2: 2,
		5: 3,
		6: 3,
	}, ring.slicesByHash)

	ring.convertHashes(3, 2, 2, true)

	require.Equal(t, map[uint64]uint64{
		1: 3,
		2: 3,
		5: 3,
		6: 3,
	}, ring.slicesByHash)
}

func TestInsertSliceNoHashes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	err = ring.insertSlice(1, "A")
	require.NoError(t, err)
	err = ring.insertSlice(2, "A")
	require.NoError(t, err)
	err = ring.insertSlice(3, "A")
	require.NoError(t, err)
	err = ring.insertSlice(4, "B")
	require.NoError(t, err)

	require.Equal(t, []uint64{1, 2, 3, 4}, ring.slices)
	require.Equal(t, map[uint64]string{
		1: "A",
		2: "A",
		3: "A",
		4: "B",
	}, ring.nodesBySlice)
}

func TestKeyEmplacement(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	// Must insert at least one slice to emplace keys.
	err = ring.insertSlice(1, "A")
	require.NoError(t, err)

	require.Equal(t, []uint64{1}, ring.slices)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "1",
			Order: 0,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "2",
			Order: 3,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "3",
			Order: 1,
		},
	})
	require.NoError(t, err)

	require.Equal(
		t,
		[]uint64{
			14180219187711517570,
			14420089009441877859,
			17062952057979069182,
		},
		ring.hashes,
	)
}

func TestKeyEmplacementAndRemovalAndSliceInsertionAndRemoval(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1

	})
	require.NoError(t, err)

	// Must insert at least one slice to emplace keys.
	err = ring.insertSlice(1, "A")
	require.NoError(t, err)

	require.Equal(t, []uint64{1}, ring.slices)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "1",
			Order: 0,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "2",
			Order: 3,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "3",
			Order: 1,
		},
	})
	require.NoError(t, err)

	require.Equal(
		t,
		[]uint64{
			14180219187711517570,
			14420089009441877859,
			17062952057979069182,
		},
		ring.hashes,
	)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 1,
		17062952057979069182: 1,
	}, ring.slicesByHash)

	err = ring.insertSlice(17062952057979069181, "B")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 1,
		17062952057979069182: 17062952057979069181,
	}, ring.slicesByHash)

	err = ring.insertSlice(0, "B")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 1,
		17062952057979069182: 17062952057979069181,
	}, ring.slicesByHash)

	ring.removeSlice(17062952057979069181)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 1,
		17062952057979069182: 1,
	}, ring.slicesByHash)

	// Test noop behavior.
	ring.removeSlice(17062952057979069181)

	err = ring.insertSlice(14420089009441877858, "B")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 14420089009441877858,
		17062952057979069182: 14420089009441877858,
	}, ring.slicesByHash)

	ring.removeSlice(14420089009441877858)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 1,
		17062952057979069182: 1,
	}, ring.slicesByHash)

	ring.removeSlice(1)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 0,
		14420089009441877859: 0,
		17062952057979069182: 0,
	}, ring.slicesByHash)

	err = ring.insertSlice(1, "B")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 1,
		17062952057979069182: 1,
	}, ring.slicesByHash)

	err = ring.insertSlice(14420089009441877859, "C")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 14420089009441877859,
		17062952057979069182: 14420089009441877859,
	}, ring.slicesByHash)

	err = ring.insertSlice(17062952057979069180, "C")
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 1,
		14420089009441877859: 14420089009441877859,
		17062952057979069182: 17062952057979069180,
	}, ring.slicesByHash)

	ring.Remove("1")

	// Test noop behavior.
	ring.Remove("1")

	require.Equal(t, map[uint64]uint64{
		14420089009441877859: 14420089009441877859,
		17062952057979069182: 17062952057979069180,
	}, ring.slicesByHash)
}

func TestCreateNode(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 2

	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)
}

func TestDeleteNode(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 2

	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	// Test noop
	ring.DeleteNode("B")

	ring.DeleteNode("A")

	err = ring.CreateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)

	ring.DeleteNode("A")

	require.Equal(t, map[string]int{
		"B": 1,
	}, ring.vFactorByNode)
}

func TestUpdateNode(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 2
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)

	require.Equal(t, map[string]int{
		"A": 1,
		"B": 1,
	}, ring.vFactorByNode)

	err = ring.UpdateNode(Node{
		Identifier: "B",
		VFactor:    2,
	})
	require.NoError(t, err)

	require.Equal(t, map[string]int{
		"A": 1,
		"B": 2,
	}, ring.vFactorByNode)

	err = ring.UpdateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)

	require.Equal(t, map[string]int{
		"A": 1,
		"B": 1,
	}, ring.vFactorByNode)

	// Test the noop behavior.
	err = ring.UpdateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)

	err = ring.UpdateNode(Node{
		Identifier: "C",
		VFactor:    2,
	})
	require.Equal(t, ErrNodeNotFound, err)
}

func TestGetNode(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 2
	})
	require.NoError(t, err)

	example := Node{
		Identifier: "A",
		VFactor:    1,
	}

	err = ring.CreateNode(example)
	require.NoError(t, err)

	node, err := ring.GetNode("A")
	require.NoError(t, err)

	require.Equal(t, example, node)

	_, err = ring.GetNode("B")
	require.Equal(t, ErrNodeNotFound, err)
}

func TestListNodes(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 2
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)

	nodes := ring.ListNodes()

	require.ElementsMatch(t, []string{"A", "B"}, nodes)
}

func TestNodeCreationAndDeletionAndKeyEmplacementAndRemoval(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 2

	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "1",
			Order: 0,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "2",
			Order: 0,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "3",
			Order: 0,
		},
	})
	require.NoError(t, err)

	require.Equal(
		t,
		map[uint64]uint64{
			14180219187711517570: 2878424575911748999,
			14420089009441877859: 2878424575911748999,
			17062952057979069182: 15603869271526861367,
		},
		ring.slicesByHash,
	)

	err = ring.CreateNode(Node{
		Identifier: "B",
		VFactor:    1,
	})
	require.NoError(t, err)

	require.Equal(t, map[uint64]uint64{
		14180219187711517570: 5509762909502811065,
		14420089009441877859: 5509762909502811065,
		17062952057979069182: 15603869271526861367,
	}, ring.slicesByHash)
}

func TestKeyAdditionAndRemovalNotification(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	c := ring.RegisterWatcher(Op[RingPayloadType]{
		Node: "A",
	})

	go func() {
		err := ring.Emplace(&Key[RingPayloadType]{
			InnerKey: &InnerKey{
				Key:   "1",
				Order: 0,
			},
		})
		require.NoError(t, err)
	}()

	require.Equal(t, Op[RingPayloadType]{
		Key:        "1",
		Node:       "A",
		Removed:    false,
		RingChange: false,
	}, <-c)

	go ring.Remove("1")

	require.Equal(t, Op[RingPayloadType]{
		Key:        "1",
		Node:       "A",
		Removed:    true,
		RingChange: false,
	}, <-c)
}

func TestRingChangeNotificationWithNotificationFiltering(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
		r.Filter = func(o Op[RingPayloadType]) string {
			return fmt.Sprintf("%s%t", o.Node, o.RingChange)
		}
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	c := ring.RegisterWatcher(Op[RingPayloadType]{
		Node:       "A",
		RingChange: true,
	})

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "1",
			Order: 0,
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "2",
			Order: 0,
		},
	})
	require.NoError(t, err)

	go func() {
		err := ring.CreateNode(Node{
			Identifier: "B",
			VFactor:    1,
		})
		require.NoError(t, err)
	}()

	require.Equal(t, Op[RingPayloadType]{
		Key:        "1",
		Node:       "A",
		Removed:    true,
		RingChange: true,
	}, <-c)

	require.Equal(t, Op[RingPayloadType]{
		Key:        "2",
		Node:       "A",
		Removed:    true,
		RingChange: true,
	}, <-c)

	go ring.DeleteNode("B")

	require.Equal(t, Op[RingPayloadType]{
		Key:        "1",
		Node:       "A",
		Removed:    false,
		RingChange: true,
	}, <-c)

	require.Equal(t, Op[RingPayloadType]{
		Key:        "2",
		Node:       "A",
		Removed:    false,
		RingChange: true,
	}, <-c)
}

func TestRingKeyOrdering(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
		r.Filter = func(o Op[RingPayloadType]) string {
			return fmt.Sprintf("%s%t", o.Node, o.RingChange)
		}
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	c := ring.RegisterWatcher(Op[RingPayloadType]{
		Node:       "A",
		RingChange: true,
	})

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "key-2",
			Order: 1,
		},
	}, "1")
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "key-1",
			Order: 0,
		},
	}, "1")
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key:   "key-3",
			Order: 3,
		},
	}, "1")
	require.NoError(t, err)

	go func() {
		err := ring.CreateNode(Node{
			Identifier: "B",
			VFactor:    1,
		})
		require.NoError(t, err)
	}()

	require.Equal(t, Op[RingPayloadType]{
		Key:        "key-1",
		Node:       "A",
		Removed:    true,
		RingChange: true,
	}, <-c)

	require.Equal(t, Op[RingPayloadType]{
		Key:        "key-2",
		Node:       "A",
		Removed:    true,
		RingChange: true,
	}, <-c)

	require.Equal(t, Op[RingPayloadType]{
		Key:        "key-3",
		Node:       "A",
		Removed:    true,
		RingChange: true,
	}, <-c)
}

func TestKeyEmplacementWithEmptyHashRing(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{InnerKey: &InnerKey{}})
	require.NoError(t, err)
	require.Equal(t, len(ring.empty), 1)
}

func TestKeyEmplacementAlreadyExistsError(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    1,
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key: "key",
		},
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key: "key",
		},
	})
	require.Equal(t, ErrKeyAlreadyExists, err)
}

func TestRegisterAndDeregisterWatcher(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	ring.RegisterWatcher(Op[RingPayloadType]{
		Key: "1",
	})

	require.Equal(t, 1, len(ring.watchers))

	ring.DeregisterWatcher(Op[RingPayloadType]{
		Key: "1",
	})

	require.Equal(t, 0, len(ring.watchers))

	// Test noop behavior.
	ring.DeregisterWatcher(Op[RingPayloadType]{
		Key: "1",
	})

	require.Equal(t, 0, len(ring.watchers))
}

func TestRingInvalidBaseVFactor(t *testing.T) {
	_, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = -1
	})
	require.Equal(t, ErrInvalidBaseVFactor, err)
}

func TestRemoveIndex(t *testing.T) {

	arr := []int{0}

	arr, err := removeIndex(arr, 0)
	require.NoError(t, err)

	// Empty removal.
	_, err = removeIndex(arr, 0)
	require.NoError(t, err)

	arr = []int{0}

	// Out of bounds.
	_, err = removeIndex(arr, 10)
	require.Equal(t, ErrOutOfBounds, err)
}

func TestFindKeyByNameEmptyKeyArray(t *testing.T) {
	require.Equal(t, -1, findKeyByName(nil, ""))
}

func TestInsertSliceAlreadyExists(t *testing.T) {
	ring, err := New[RingPayloadType]()
	require.NoError(t, err)
	require.NoError(t, ring.insertSlice(0, "A"))
	require.Equal(t, ErrSliceAlreadyExists, ring.insertSlice(0, "A"))
}

func TestCreateNodeAlreadyExists(t *testing.T) {
	ring, err := New[RingPayloadType]()
	require.NoError(t, err)
	require.NoError(t, ring.CreateNode(Node{
		Identifier: "A",
	}))
	require.Equal(t, ErrNodeAlreadyExists, ring.CreateNode(Node{
		Identifier: "A",
	}))
}

func TestKeyEmplacementAndRemovalWithEmptyHashRing(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{InnerKey: &InnerKey{Key: "0"}})
	require.NoError(t, err)
	require.Equal(t, len(ring.empty), 1)

	ring.Remove("0")
	require.Equal(t, len(ring.empty), 0)
}

func TestKeyEmplacementWithEmptyHashRingAndInsertFirstSlice(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{InnerKey: &InnerKey{Key: "0"}})
	require.NoError(t, err)
	require.Equal(t, len(ring.empty), 1)

	err = ring.insertSlice(0, "0")
	require.NoError(t, err)
	require.Equal(t, len(ring.empty), 0)

	require.Equal(t, uint64(0), ring.slicesByHash[ring.hashesByKey["0"]])
}

func TestRingFinalSliceRemoval(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.insertSlice(0, "0")
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{InnerKey: &InnerKey{Key: "0"}})
	require.NoError(t, err)

	ring.removeSlice(0)
	require.Equal(t, len(ring.empty), 1)
}

func TestFinalSliceRemovalAndAddition(t *testing.T) {
	ring, err := New(func(r *Ring[RingPayloadType]) {
		r.BaseVFactor = 1
	})
	require.NoError(t, err)

	err = ring.insertSlice(0, "0")
	require.NoError(t, err)

	err = ring.Emplace(&Key[RingPayloadType]{InnerKey: &InnerKey{Key: "0"}})
	require.NoError(t, err)

	ring.removeSlice(0)
	require.Equal(t, len(ring.empty), 1)

	err = ring.insertSlice(0, "0")
	require.NoError(t, err)
	require.Equal(t, len(ring.empty), 0)
}

func TestKeyUpdates(t *testing.T) {
	id := "0"

	ring, err := New[RingPayloadType](func(r *Ring[RingPayloadType]) {
		r.Filter = func(o Op[RingPayloadType]) string {
			return fmt.Sprintf("%s%t", o.Node, o.Updated)
		}
	})
	require.NoError(t, err)

	err = ring.CreateNode(Node{
		Identifier: id,
		VFactor:    1,
	})
	require.NoError(t, err)

	done := make(chan struct{})

	ops := ring.RegisterWatcher(Op[RingPayloadType]{
		Updated: true,
		Node:    id,
	})
	go func() {
		op := <-ops
		require.Equal(t, op, Op[RingPayloadType]{
			Key:     id,
			Node:    id,
			Updated: true,
		})
		close(done)
	}()

	err = ring.Emplace(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key: id,
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(ring.contentByKey))

	err = ring.Update(&Key[RingPayloadType]{
		InnerKey: &InnerKey{
			Key: id,
		},
	})
	require.NoError(t, err)

	ring.Remove(id)

	require.Equal(t, 0, len(ring.contentByKey))

	<-done
}
