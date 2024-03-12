package ring

// MD5 is used for hashing only, and not for any cryptographic/security related functionality.
import (
	"crypto/md5" // #nosec G501
	"encoding/binary"
)

// MD5 uses the MD5 hashing algorithm to hash an identifier into a uint64.
func MD5(identifier string) uint64 {
	hash := md5.Sum([]byte(identifier)) // #nosec G401
	hashSlice := hash[:]
	return binary.BigEndian.Uint64(hashSlice)
}