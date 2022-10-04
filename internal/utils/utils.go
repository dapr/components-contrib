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

// GetElemOrDefaultFromMap returns the value of a key from a map, or a default value
// if the key does not exist or the value is not of the expected type.
func GetElemOrDefaultFromMap[T int | uint64](m map[string]string, key string, def T) T {
	if val, ok := m[key]; ok {
		switch any(def).(type) {
		case int:
			if ival, err := strconv.ParseInt(val, 10, 64); err == nil {
				return T(ival)
			}
		case uint64:
			if uval, err := strconv.ParseUint(val, 10, 64); err == nil {
				return T(uval)
			}
		}
	}
	return def
}
