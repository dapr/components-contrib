package utils

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
)

type CommonConfig struct {
	ComponentType string
	ComponentName string
	AllOperations bool
	Operations    sets.String
}

func (cc CommonConfig) HasOperation(operation string) bool {
	return cc.AllOperations || cc.Operations.Has(operation)
}

func (cc CommonConfig) GetTestName(operation string) string {
	return fmt.Sprintf("%s/%s/%s", cc.ComponentType, cc.ComponentName, operation)
}
