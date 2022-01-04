package zeebe

import (
	"strings"
)

func VariableStringToArray(variableString string) []string {
	return strings.Split(strings.ReplaceAll(variableString, " ", ""), ",")
}
