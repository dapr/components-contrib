package tablestorage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetadataRequirements(t *testing.T) {
	t.Run("Nothing at all", func(t *testing.T) {
		m := make(map[string]string)
		_, err := getTablesMetadata(m)

		assert.NotNil(t, err)
	})
}
