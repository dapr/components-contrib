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

// GetIntOrDefFromMap returns the value of a key in a map as an int,
// or a default value if the key is not present or a valid int.
func GetIntOrDefFromMap(m map[string]string, key string, def int) int {
	if val, ok := m[key]; ok {
		if ival, err := strconv.Atoi(val); err == nil {
			return ival
		}
	}
	return def
}
