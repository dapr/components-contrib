package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQuerySelectedAttributes(t *testing.T) {
	t.Run("Selected attributes no empty ", func(t *testing.T) {
		selectedAttributes := `[{"name":"test", "path":"data.test"}]`

		attributeArray := []Attribute{{Name: "test", Path: "data.test"}}

		querySelectedAttributes, err := ParseQuerySelectedAttributes(selectedAttributes)
		require.NoError(t, err)
		assert.Equal(t, querySelectedAttributes, attributeArray)
	})
	t.Run("Selected attributes empty ", func(t *testing.T) {
		selectedAttributes := ``

		querySelectedAttributes, err := ParseQuerySelectedAttributes(selectedAttributes)
		require.NoError(t, err)
		assert.Nil(t, querySelectedAttributes)
	})

	t.Run("Selected attributes wrong syntax ", func(t *testing.T) {
		selectedAttributes := `[{"name":"test", "path":"data.test"`
		querySelectedAttributes, err := ParseQuerySelectedAttributes(selectedAttributes)
		require.Error(t, err)
		assert.Nil(t, querySelectedAttributes)
	})

	t.Run("Selected attributes no matching schema ", func(t *testing.T) {
		selectedAttributes := `[{"name":"test", "pat":"data.test"}]`
		querySelectedAttributes, err := ParseQuerySelectedAttributes(selectedAttributes)
		require.Error(t, err)
		assert.Nil(t, querySelectedAttributes)
	})
}
