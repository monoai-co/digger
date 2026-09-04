package operation

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"strings"
)

const (
	ProtocolVersion = 1
	idPrefix        = "op1_"
)

// ID is a stable identity for one logical control-plane operation. It is safe
// to include in provider idempotency markers and workflow inputs.
type ID string

var ErrInvalidComponent = errors.New("operation identity components must not be empty")

// Derive hashes versioned, length-prefixed components. Length prefixes prevent
// concatenation ambiguity while the namespace and version permit later schemes.
func Derive(kind string, components ...string) (ID, error) {
	if strings.TrimSpace(kind) == "" {
		return "", ErrInvalidComponent
	}
	for _, component := range components {
		if component == "" {
			return "", ErrInvalidComponent
		}
	}

	digest := sha256.New()
	writeComponent(digest, "digger-control-operation")
	writeComponent(digest, "v1")
	writeComponent(digest, kind)
	for _, component := range components {
		writeComponent(digest, component)
	}
	return ID(idPrefix + hex.EncodeToString(digest.Sum(nil))), nil
}

func (id ID) String() string {
	return string(id)
}

func (id ID) Valid() bool {
	value := string(id)
	if !strings.HasPrefix(value, idPrefix) || len(value) != len(idPrefix)+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, idPrefix))
	return err == nil
}

func writeComponent(digest hash.Hash, component string) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(component)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write([]byte(component))
}
