// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bpf

import (
	"testing"

	"github.com/cilium/ebpf"
	"github.com/dapr/components-contrib/state"
	"github.com/stretchr/testify/assert"
)

func TestBpfReadWrite(t *testing.T) {
	t.Run("storing a string", func(t *testing.T) {
		expected := "foo bar qaz"
		m, err := ebpf.NewMap(&ebpf.MapSpec{
			Type:       ebpf.Hash,
			KeySize:    4,
			ValueSize:  uint32(len(expected)),
			MaxEntries: 1,
		})
		assert.Equal(t, nil, err)
		defer m.Close()
		if err := m.Put(uint32(0), expected); err != nil {
			t.Fatal("Can't put:", err)
		}
		var bt []byte = make([]byte, len(expected))
		if err := m.Lookup(uint32(0), &bt); err != nil {
			t.Fatal("Can't lookup 0:", err)
		}
		actual := string(bt)
		assert.Equal(t, expected, actual)
	})
}

func TestValidateMetadata(t *testing.T) {
	t.Run("with mandatory fields", func(t *testing.T) {
		props := map[string]string{
			valueSize:  "4",
			maxEntries: "100",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.Equal(t, nil, err)
	})
	t.Run("with missing keySize", func(t *testing.T) {
		props := map[string]string{
			valueSize:  "4",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
	t.Run("with missing valueSize", func(t *testing.T) {
		props := map[string]string{
			maxEntries: "100",
		}
		metadata := state.Metadata{Properties: props}
		err := validateMetadata(metadata)
		assert.NotNil(t, err)
	})
}
