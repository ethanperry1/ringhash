package ring

type MockRing[T any] struct {
	OnRegisterWatcher   func(filter Op[T]) chan Op[T]
	OnDeregisterWatcher func(op Op[T])
	OnEmplace           func(key *Key[T], hk ...string) error
	OnUpdate            func(key *Key[T]) error
	OnRemove            func(string)
	OnCreateNode        func(node Node) error
	OnDeleteNode        func(identifier string)
	OnUpdateNode        func(node Node) error
	OnGetNode           func(identifier string) (Node, error)
	OnListNodes         func() []string
	OnState             func() *State
}

func (ring *MockRing[T]) RegisterWatcher(filter Op[T]) chan Op[T] {
	return ring.OnRegisterWatcher(filter)
}

func (ring *MockRing[T]) DeregisterWatcher(op Op[T]) {
	ring.OnDeregisterWatcher(op)
}

func (ring *MockRing[T]) Emplace(key *Key[T], hk ...string) error {
	return ring.OnEmplace(key, hk...)
}

func (ring *MockRing[T]) Update(key *Key[T]) error {
	return ring.OnUpdate(key)
}

func (ring *MockRing[T]) Remove(s string) {
	ring.OnRemove(s)
}

func (ring *MockRing[T]) CreateNode(node Node) error {
	return ring.OnCreateNode(node)
}

func (ring *MockRing[T]) DeleteNode(identifier string) {
	ring.OnDeleteNode(identifier)
}

func (ring *MockRing[T]) UpdateNode(node Node) error {
	return ring.OnUpdateNode(node)

}
func (ring *MockRing[T]) GetNode(identifier string) (Node, error) {
	return ring.OnGetNode(identifier)
}

func (ring *MockRing[T]) ListNodes() []string {
	return ring.OnListNodes()
}

func (ring *MockRing[T]) State() *State {
	return ring.OnState()
}
