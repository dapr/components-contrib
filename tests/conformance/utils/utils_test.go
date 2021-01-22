package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestHasOperation(t *testing.T) {
	t.Run("all operations", func(t *testing.T) {
		cc := CommonConfig{
			ComponentType: "state",
			ComponentName: "redis",
			AllOperations: true,
		}
		assert.True(t, cc.HasOperation("op"))
	})
	t.Run("operations list", func(t *testing.T) {
		cc := CommonConfig{
			ComponentType: "state",
			ComponentName: "redis",
			Operations:    sets.NewString("op1", "op2"),
		}
		assert.True(t, cc.HasOperation("op1"))
		assert.True(t, cc.HasOperation("op2"))
		assert.False(t, cc.HasOperation("op3"))
	})
}

func TestCopyMap(t *testing.T) {
	cc := CommonConfig{
		ComponentType: "state",
		ComponentName: "redis",
		AllOperations: true,
	}
	in := map[string]string{
		"k": "v",
		"v": "k",
	}
	out := cc.CopyMap(in)
	assert.Equal(t, in, out)
	assert.NotEqual(t, reflect.ValueOf(in).Pointer(), reflect.ValueOf(out).Pointer())
}
