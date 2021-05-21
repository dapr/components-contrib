package config

import (
	"strings"
	"unicode"
)

func PrefixedBy(input interface{}, prefix string) (interface{}, error) {
	normalized, err := Normalize(input)
	if err != nil {
		// The only error that can come from normalize is if
		// input is a map[interface{}]interface{} and contains
		// a key that is not a string.
		return input, err
	}
	input = normalized

	if inputMap, ok := input.(map[string]interface{}); ok {
		converted := make(map[string]interface{}, len(inputMap))
		for k, v := range inputMap {
			if strings.HasPrefix(k, prefix) {
				key := uncapitalize(strings.TrimPrefix(k, prefix))
				converted[key] = v
			}
		}

		return converted, nil
	} else if inputMap, ok := input.(map[string]string); ok {
		converted := make(map[string]string, len(inputMap))
		for k, v := range inputMap {
			if strings.HasPrefix(k, prefix) {
				key := uncapitalize(strings.TrimPrefix(k, prefix))
				converted[key] = v
			}
		}

		return converted, nil
	}

	return input, nil
}

// uncapitalize initial capital letters in `str`.
func uncapitalize(str string) string {
	if len(str) == 0 {
		return str
	}

	vv := []rune(str) // Introduced later
	vv[0] = unicode.ToLower(vv[0])

	return string(vv)
}
