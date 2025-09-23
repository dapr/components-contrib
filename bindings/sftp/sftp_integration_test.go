package sftp

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
)

var connectionStringEnvKey = "DAPR_TEST_SFTP_CONNSTRING"

// Run docker from the file location as the upload folder is relative to the test
// docker run -v ./upload:/home/foo/upload -p 2222:22 -d atmoz/sftp foo:pass:1001
func TestIntegrationCases(t *testing.T) {
	connectionString := os.Getenv(connectionStringEnvKey)
	if connectionString == "" {
		t.Skipf(`sftp binding integration tests skipped. To enable this test, define the connection string using environment variable '%[1]s' (example 'export %[1]s="localhost:2222")'`, connectionStringEnvKey)
	}

	t.Run("List operation", testListOperation)
	t.Run("Create operation", testCreateOperation)
}

func testListOperation(t *testing.T) {
	c := Sftp{}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"rootPath":              "/upload",
		"address":               os.Getenv(connectionStringEnvKey),
		"username":              "foo",
		"password":              "pass",
		"insecureIgnoreHostKey": "true",
	}
	err := c.Init(t.Context(), m)
	require.NoError(t, err)

	r, err := c.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})
	require.NoError(t, err)
	assert.NotNil(t, r.Data)

	var d []listResponse
	err = json.Unmarshal(r.Data, &d)
	require.NoError(t, err)
}

func testCreateOperation(t *testing.T) {
	c := Sftp{}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"rootPath":              "/upload",
		"address":               os.Getenv(connectionStringEnvKey),
		"username":              "foo",
		"password":              "pass",
		"insecureIgnoreHostKey": "true",
	}

	err := os.Remove("./upload/test.txt")
	if err != nil && !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	err = c.Init(t.Context(), m)
	require.NoError(t, err)

	r, err := c.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte("test data 1"),
		Metadata: map[string]string{
			"fileName": "test.txt",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, r.Data)

	file, err := os.Stat("./upload/test.txt")
	require.NoError(t, err)
	assert.Equal(t, "test.txt", file.Name())
}
