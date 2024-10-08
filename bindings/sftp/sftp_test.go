package sftp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
)

func TestParseMeta(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"rootPath":              "path",
			"address":               "address",
			"username":              "user",
			"password":              "pass",
			"privateKey":            "cHJpdmF0ZUtleQ==",
			"hostPublicKey":         "aG9zdFB1YmxpY0tleQ==",
			"KnownHostsFile":        "/known_hosts",
			"insecureIgnoreHostKey": "true",
		}
		sftp := Sftp{}
		meta, err := sftp.parseMetadata(m)

		privateKeyBytes := []byte{0x63, 0x48, 0x4a, 0x70, 0x64, 0x6d, 0x46, 0x30, 0x5a, 0x55, 0x74, 0x6c, 0x65, 0x51, 0x3d, 0x3d}
		hostPublicKeyBytes := []byte{0x61, 0x47, 0x39, 0x7a, 0x64, 0x46, 0x42, 0x31, 0x59, 0x6d, 0x78, 0x70, 0x59, 0x30, 0x74, 0x6c, 0x65, 0x51, 0x3d, 0x3d}

		require.NoError(t, err)
		assert.Equal(t, "path", meta.RootPath)
		assert.Equal(t, "address", meta.Address)
		assert.Equal(t, "user", meta.Username)
		assert.Equal(t, "pass", meta.Password)
		assert.Equal(t, privateKeyBytes, meta.PrivateKey)
		assert.Equal(t, hostPublicKeyBytes, meta.HostPublicKey)
		assert.Equal(t, "/known_hosts", meta.KnownHostsFile)
		assert.True(t, meta.InsecureIgnoreHostKey)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"rootPath":              "path",
			"address":               "address",
			"username":              "user",
			"password":              "pass",
			"privateKey":            "cHJpdmF0ZUtleQ==",
			"hostPublicKey":         "aG9zdFB1YmxpY0tleQ==",
			"KnownHostsFile":        "/known_hosts",
			"insecureIgnoreHostKey": "true",
		}
		sftp := Sftp{}
		meta, _ := sftp.parseMetadata(m)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"rootPath":       "changedpath",
			"address":        "changedaddress",
			"username":       "changeduser",
			"password":       "changedpass",
			"privateKey":     "changedcHJpdmF0ZUtleQ==",
			"hostPublicKey":  "changedaG9zdFB1YmxpY0tleQ==",
			"KnownHostsFile": "changed/known_hosts",
			"insecureSSL":    "changedtrue",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		privateKeyBytes := []byte{0x63, 0x48, 0x4a, 0x70, 0x64, 0x6d, 0x46, 0x30, 0x5a, 0x55, 0x74, 0x6c, 0x65, 0x51, 0x3d, 0x3d}
		hostPublicKeyBytes := []byte{0x61, 0x47, 0x39, 0x7a, 0x64, 0x46, 0x42, 0x31, 0x59, 0x6d, 0x78, 0x70, 0x59, 0x30, 0x74, 0x6c, 0x65, 0x51, 0x3d, 0x3d}

		require.NoError(t, err)
		assert.Equal(t, "changedpath", mergedMeta.RootPath)
		assert.Equal(t, "address", mergedMeta.Address)
		assert.Equal(t, "user", mergedMeta.Username)
		assert.Equal(t, "pass", mergedMeta.Password)
		assert.Equal(t, privateKeyBytes, mergedMeta.PrivateKey)
		assert.Equal(t, hostPublicKeyBytes, mergedMeta.HostPublicKey)
		assert.Equal(t, "/known_hosts", mergedMeta.KnownHostsFile)
		assert.True(t, meta.InsecureIgnoreHostKey)
	})
}

func TestGetPath(t *testing.T) {
	t.Run("has path", func(t *testing.T) {
		m := &sftpMetadata{
			RootPath: "/path",
		}
		_, err := m.getPath(map[string]string{})
		require.NoError(t, err)
	})

	t.Run("return if error path is empty", func(t *testing.T) {
		m := &sftpMetadata{}
		_, err := m.getPath(map[string]string{})
		require.Error(t, err)
	})
}
