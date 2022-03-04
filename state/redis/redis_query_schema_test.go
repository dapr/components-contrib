package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingEmptySchema(t *testing.T) {
	schemas, err := parseQuerySchemas("")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(schemas))
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
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schemas))
	assert.Equal(t,
		schemas["schema1"].keys,
		map[string]string{"person.org": "var0", "person.id": "var1", "city": "var2", "state": "var3"})
	assert.Equal(t,
		schemas["schema1"].schema,
		[]interface{}{
			"FT.CREATE", "schema1", "ON", "JSON", "SCHEMA",
			"$.data.person.org", "AS", "var0", "TEXT", "SORTABLE",
			"$.data.person.id", "AS", "var1", "NUMERIC", "SORTABLE",
			"$.data.city", "AS", "var2", "TEXT", "SORTABLE",
			"$.data.state", "AS", "var3", "TEXT", "SORTABLE",
		})
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
	assert.NoError(t, err)
	assert.Equal(t, 2, len(schemas))
	assert.Equal(t,
		schemas["schema1"].keys,
		map[string]string{"person.org": "var0", "city": "var1"})
	assert.Equal(t,
		schemas["schema1"].schema,
		[]interface{}{
			"FT.CREATE", "schema1", "ON", "JSON", "SCHEMA",
			"$.data.person.org", "AS", "var0", "TEXT", "SORTABLE",
			"$.data.city", "AS", "var1", "TEXT", "SORTABLE",
		})
	assert.Equal(t,
		schemas["schema2"].keys,
		map[string]string{"person.id": "var0", "state": "var1"})
	assert.Equal(t,
		schemas["schema2"].schema,
		[]interface{}{
			"FT.CREATE", "schema2", "ON", "JSON", "SCHEMA",
			"$.data.person.id", "AS", "var0", "NUMERIC", "SORTABLE",
			"$.data.state", "AS", "var1", "TEXT", "SORTABLE",
		})
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
		assert.EqualError(t, err, test.err)
	}
}
