package utils

import (
	"strconv"
	"strings"
)

// IsTruthy returns true if a string is a truthy value.
// Truthy values are "y", "yes", "true", "t", "on", "1" (case-insensitive); everything else is false.
func IsTruthy(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "y", "yes", "true", "t", "on", "1":
		return true
	default:
		return false
	}
}

// GetUint64OrDefFromMap returns the value of a key in a map as a uint64,
// or a default value if the key is not present or a valid uint64.
func GetUint64OrDefFromMap(m map[string]string, key string, def uint64) uint64 {
	if val, ok := m[key]; ok {
		if uval, err := strconv.ParseUint(val, 10, 64); err == nil {
			return uval
		}
	}
	return def
}
