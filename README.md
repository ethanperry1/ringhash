# Ringhash

Ringhash is a golang implementation of a ring hash data structure, with some additional special features:
1. Generic key-value store
1. Channel based notification system
1. Optionally duplicate hashing keys
1. Ordering mechanism for channel notifications
1. Concurrency safe for every method
1. "Empty Node," where keys are assigned when no nodes are present
1. Virtual factor (vfactor) to increase key distribution amoung nodes
1. Zero non-testing dependencies

## Quickstart

```go
import (
    "github.com/ethanperry1/ringhash"
)

func main() {
    ring, err := ringhash.New()
    if err != nil {
        // Handle error here -- error will be returned if vFactor is set to any value less than 1.
    }

    err = ring.CreateNode(Node{
		Identifier: "A",
		VFactor:    100,
	})
    if err != nil { /* Handle error */ }

    err = ring.Emplace(&Key[int]{
		InnerKey: &InnerKey{
			Key:   "2", // Key must be unique.
			Order: 0,
		}, Value: 184, // Value associated with this key.
	},
        "hash_key" // This optional key will be used to hash the key into the ring, and does not need to be unique.
    )
}
```