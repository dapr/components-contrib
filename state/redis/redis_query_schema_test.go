package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsingEmptySchema(t *testing.T) {
	schemas, err := parseQuerySchemas("")
	require.NoError(t, err)
	assert.Empty(t, schemas)
}

func TestParsingSingleSchema(t *testing.T) {
	content := `
[
    {
        "name": "schema1",
        "indexes": [
            {
                "key": "person.org",
                "type": "TEXT"
            },
            {
                "key": "person.id",
                "type": "NUMERIC"
            },
            {
                "key": "city",
                "type": "TEXT"
            },
            {
                "key": "state",
                "type": "TEXT"
            }
        ]
    }
]`
	schemas, err := parseQuerySchemas(content)
	require.NoError(t, err)
	assert.Len(t, schemas, 1)
	assert.Equal(t,
		map[string]string{"person.org": "var0", "person.id": "var1", "city": "var2", "state": "var3"},
		schemas["schema1"].keys)
	assert.Equal(t,
		[]interface{}{
			"FT.CREATE", "schema1", "ON", "JSON", "SCHEMA",
			"$.data.person.org", "AS", "var0", "TEXT", "SORTABLE",
			"$.data.person.id", "AS", "var1", "NUMERIC", "SORTABLE",
			"$.data.city", "AS", "var2", "TEXT", "SORTABLE",
			"$.data.state", "AS", "var3", "TEXT", "SORTABLE",
		}, schemas["schema1"].schema)
}

func TestParsingMultiSchema(t *testing.T) {
	content := `
[
    {
        "name": "schema1",
        "indexes": [
            {
                "key": "person.org",
                "type": "TEXT"
            },
            {
                "key": "city",
                "type": "TEXT"
            }
        ]
    },
	{
        "name": "schema2",
        "indexes": [
            {
                "key": "person.id",
                "type": "NUMERIC"
            },
            {
                "key": "state",
                "type": "TEXT"
            }
        ]
    }
]`
	schemas, err := parseQuerySchemas(content)
	require.NoError(t, err)
	assert.Len(t, schemas, 2)
	assert.Equal(t,
		map[string]string{"person.org": "var0", "city": "var1"},
		schemas["schema1"].keys)
	assert.Equal(t,
		[]interface{}{
			"FT.CREATE", "schema1", "ON", "JSON", "SCHEMA",
			"$.data.person.org", "AS", "var0", "TEXT", "SORTABLE",
			"$.data.city", "AS", "var1", "TEXT", "SORTABLE",
		}, schemas["schema1"].schema)
	assert.Equal(t,
		map[string]string{"person.id": "var0", "state": "var1"},
		schemas["schema2"].keys)
	assert.Equal(t,
		[]interface{}{
			"FT.CREATE", "schema2", "ON", "JSON", "SCHEMA",
			"$.data.person.id", "AS", "var0", "NUMERIC", "SORTABLE",
			"$.data.state", "AS", "var1", "TEXT", "SORTABLE",
		}, schemas["schema2"].schema)
}

func TestParsingSchemaErrors(t *testing.T) {
	tests := []struct{ content, err string }{
		{
			content: `
			[
				{
					"name": "schema1",
					"indexes": [
						{
							"type": "TEXT"
						}
					]
				}
			]`,
			err: "empty key in query schema schema1",
		},
		{
			content: `
			[
				{
					"name": "schema3",
					"indexes": [
						{
							"key": "state"
						}
					]
				}
			]`,
			err: "empty type in query schema schema3",
		},
	}

	for _, test := range tests {
		_, err := parseQuerySchemas(test.content)
		require.EqualError(t, err, test.err)
	}
}
