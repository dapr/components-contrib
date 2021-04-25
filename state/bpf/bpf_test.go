// +build linux
// +build amd64

package bpf

import (
	"testing"

	"github.com/cilium/ebpf"
	"github.com/stretchr/testify/assert"
)

func TestBpfReadWrite(t *testing.T) {
	t.Run("store a string and pin to file", func(t *testing.T) {
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
		m.Pin("/sys/fs/bpf/foo")
	})
	t.Run("load and read from a pinned file", func(t *testing.T) {
		expected := "foo bar qaz"
		m, err := ebpf.LoadPinnedMap("/sys/fs/bpf/foo", &ebpf.LoadPinOptions{ReadOnly: true})
		assert.Equal(t, nil, err)
		defer m.Close()
		var bt []byte = make([]byte, len(expected))
		if err := m.Lookup(uint32(0), &bt); err != nil {
			t.Fatal("Can't lookup 0:", err)
		}
		actual := string(bt)
		assert.Equal(t, expected, actual)
	})
}
