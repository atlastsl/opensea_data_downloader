package utils

import (
	"crypto/sha256"
	"encoding/hex"
)

func CreateHash(plain string) string {
	hasher := sha256.New()
	hasher.Write([]byte(plain))
	return hex.EncodeToString(hasher.Sum(nil))
}
