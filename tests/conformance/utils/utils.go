package utils

import (
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

func (cc CommonConfig) CopyMap(config map[string]string) map[string]string {
	m := map[string]string{}
	for k, v := range config {
		m[k] = v
	}

	return m
}
