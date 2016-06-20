package uuid

import (
	"crypto/rand"
	"fmt"
	"log"
)

// read random bytes from a csprng or panic
func safeRandom(dst []byte) {
	if _, err := rand.Read(dst); err != nil {
		log.Fatalf("uuid: failed reading random bytes in call to New: %s", err)
	}
}

// New returns a new Version 4 (random) UUID in canonical string form
func New() string {
	u := make([]byte, 16)
	safeRandom(u)
	// set version bits
	u[6] = (u[6] & 0x0f) | 0x40
	// set variant bits
	u[8] = (u[8] & 0xbf) | 0x80
	// return string representation
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}
