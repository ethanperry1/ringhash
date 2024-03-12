package ring

import (
	"errors"
)

var (
	ErrInvalidBaseVFactor = errors.New(
		"base vFactor for ring hash cannot be less than one",
	)
	ErrNodeNotFound = errors.New(
		"the node with this identifier could not be found",
	)
	ErrSliceHashCollision = errors.New(
		"a slice hash collision occurred",
	)
	ErrOutOfBounds = errors.New(
		"attempted to remove index out of bounds",
	)
	ErrKeyAlreadyExists = errors.New(
		"key with this identifier already exists",
	)
	ErrNilKey = errors.New(
		"key cannot be nil",
	)
	ErrKeyNotFound = errors.New(
		"key with this identifier could not be found",
	)
	ErrNodeAlreadyExists = errors.New(
		"node with this identifier already exists",
	)
	ErrSliceAlreadyExists = errors.New(
		"slice with this identifier already exists",
	)
)
