package objectstorage

// run the test for example in ~/dapr-dev/components-contrib
// go test -v github.com/dapr/components-contrib/state/oci/objectstorage.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	configFilePathEnvKey    = "DAPR_TEST_OCI_CONFIG_FILE_PATH"
	bucketNameEnvKey        = "DAPR_TEST_OCI_BUCKET_NAME"
	compartmentOCIDEnvKey   = "DAPR_TEST_OCI_COMPARTMENT_OCID"
	configFileProfileEnvKey = "DAPR_TEST_OCI_CONFIG_PROFILE"
)

func getConfigFilePathString() string {
	return os.Getenv(configFilePathEnvKey)
}

func getConfigProfile() string {
	return os.Getenv(configFileProfileEnvKey)
}

func getCompartmentOCID() string {
	return os.Getenv(compartmentOCIDEnvKey)
}

func getBucketName() string {
	return os.Getenv(bucketNameEnvKey)
}

func TestOCIObjectStorageIntegration(t *testing.T) {
	ociObjectStorageConfiguration := map[string]string{
		"configFileAuthentication": "true",
	}
	configFilePath := getConfigFilePathString()
	if configFilePath == "" {
		// first run export DAPR_TEST_OCI_CONFIG_FILE_PATH="/home/app/.oci/config".
		t.Skipf("OCI ObjectStorage state integration tests skipped. To enable define the configuration file path string using environment variable '%s' (example 'export %s=\"/home/app/.oci/config\")", configFilePathEnvKey, configFilePathEnvKey)
	}
	ociObjectStorageConfiguration["configFilePath"] = configFilePath
	ociObjectStorageConfiguration["configFileProfile"] = getConfigProfile()

	compartmentOCID := getCompartmentOCID()
	if compartmentOCID == "" {
		t.Skipf("OCI ObjectStorage state integration tests skipped. To enable define the compartment OCID string using environment variable '%s' (example 'export %s=\"ocid1.compartment.oc1..aaaaaaaacsssekayq4d34nl5h3eqs5e6ak3j5s4jhlws7rr5pxmt3zrq\")", compartmentOCIDEnvKey, compartmentOCIDEnvKey)
	}
	ociObjectStorageConfiguration["compartmentOCID"] = compartmentOCID

	bucketName := getBucketName()
	if bucketName == "" {
		bucketName = "DAPR_TEST_BUCKET"
	}
	ociObjectStorageConfiguration["bucketName"] = bucketName

	t.Run("Test Get", func(t *testing.T) {
		t.Parallel()
		testGet(t, ociObjectStorageConfiguration)
	})
	t.Run("Test Set", func(t *testing.T) {
		t.Parallel()
		testSet(t, ociObjectStorageConfiguration)
	})
	t.Run("Test Delete", func(t *testing.T) {
		t.Parallel()
		testDelete(t, ociObjectStorageConfiguration)
	})
	t.Run("Test Ping", func(t *testing.T) {
		t.Parallel()
		testPing(t, ociObjectStorageConfiguration)
	})
}

func testGet(t *testing.T, ociProperties map[string]string) {
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	meta := state.Metadata{}
	meta.Properties = ociProperties

	t.Run("Get an non-existing key", func(t *testing.T) {
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: "xyzq"})
		assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
		require.NoError(t, err, "Non-existing key must not be treated as error")
	})
	t.Run("Get an existing key", func(t *testing.T) {
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		err = statestore.Set(context.Background(), &state.SetRequest{Key: "test-key", Value: []byte("test-value")})
		require.NoError(t, err)
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: "test-key"})
		require.NoError(t, err)
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
	})
	t.Run("Get an existing composed key", func(t *testing.T) {
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		err = statestore.Set(context.Background(), &state.SetRequest{Key: "test-app||test-key", Value: []byte("test-value")})
		require.NoError(t, err)
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: "test-app||test-key"})
		require.NoError(t, err)
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
	})
	t.Run("Get an unexpired state element with TTL set", func(t *testing.T) {
		testKey := "unexpired-ttl-test-key"
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "100",
		})})
		require.NoError(t, err)
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: testKey})
		require.NoError(t, err)
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set despite TTL setting")
	})
	t.Run("Get a state element with TTL set to -1 (not expire)", func(t *testing.T) {
		testKey := "never-expiring-ttl-test-key"
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "-1",
		})})
		require.NoError(t, err)
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: testKey})
		require.NoError(t, err)
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal (TTL setting of -1 means never expire)")
	})
	t.Run("Get an expired (TTL in the past) state element", func(t *testing.T) {
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		err = statestore.Set(context.Background(), &state.SetRequest{Key: "ttl-test-key", Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "1",
		})})
		require.NoError(t, err)
		time.Sleep(time.Second * 2)
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: "ttl-test-key"})
		assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
		require.NoError(t, err, "Expired element must not be treated as error")
	})
}

func testSet(t *testing.T, ociProperties map[string]string) {
	meta := state.Metadata{}
	meta.Properties = ociProperties
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Set without a key", func(t *testing.T) {
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)
		err = statestore.Set(context.Background(), &state.SetRequest{Value: []byte("test-value")})
		assert.Equal(t, err, errors.New("key for value to set was missing from request"), "Lacking Key results in error")
	})
	t.Run("Regular Set Operation", func(t *testing.T) {
		testKey := "local-test-key"
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper key should be errorfree")
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: testKey})
		require.NoError(t, err)
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
	})
	t.Run("Regular Set Operation with composite key", func(t *testing.T) {
		testKey := "test-app||other-test-key"
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper composite key should be errorfree")
		getResponse, err := statestore.Get(context.Background(), &state.GetRequest{Key: testKey})
		require.NoError(t, err)
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
	})
	t.Run("Regular Set Operation with TTL", func(t *testing.T) {
		testKey := "test-key-with-ttl"
		err := statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "500",
		})})
		require.NoError(t, err, "Setting a value with a proper key and a correct TTL value should be errorfree")
		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "XXX",
		})})
		require.Error(t, err, "Setting a value with a proper key and a incorrect TTL value should be produce an error")
	})

	t.Run("Testing Set & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-key"
		err := statestore.Init(context.Background(), meta)
		require.NoError(t, err)

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper key should be errorfree")
		getResponse, _ := statestore.Get(context.Background(), &state.GetRequest{Key: testKey})
		etag := getResponse.ETag
		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.NoError(t, err, "Updating value with proper etag should go fine")

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("more-overwritten-value"), ETag: etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Updating value with the old etag should be refused")

		// retrieve the latest etag - assigned by the previous set operation.
		getResponse, _ = statestore.Get(context.Background(), &state.GetRequest{Key: testKey})
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
		etag = getResponse.ETag
		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("more-overwritten-value"), ETag: etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.NoError(t, err, "Updating value with the latest etag should be accepted")
	})
}

func testDelete(t *testing.T, ociProperties map[string]string) {
	m := state.Metadata{}
	m.Properties = ociProperties
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Delete without a key", func(t *testing.T) {
		err := s.Init(context.Background(), m)
		require.NoError(t, err)
		err = s.Delete(context.Background(), &state.DeleteRequest{})
		assert.Equal(t, err, errors.New("key for value to delete was missing from request"), "Lacking Key results in error")
	})
	t.Run("Regular Delete Operation", func(t *testing.T) {
		testKey := "test-key"
		err := s.Init(context.Background(), m)
		require.NoError(t, err)

		err = s.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper key should be errorfree")
		err = s.Delete(context.Background(), &state.DeleteRequest{Key: testKey})
		require.NoError(t, err, "Deleting an existing value with a proper key should be errorfree")
	})
	t.Run("Regular Delete Operation for composite key", func(t *testing.T) {
		testKey := "test-app||some-test-key"
		err := s.Init(context.Background(), m)
		require.NoError(t, err)

		err = s.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper composite key should be errorfree")
		err = s.Delete(context.Background(), &state.DeleteRequest{Key: testKey})
		require.NoError(t, err, "Deleting an existing value with a proper composite key should be errorfree")
	})
	t.Run("Delete with an unknown key", func(t *testing.T) {
		err := s.Delete(context.Background(), &state.DeleteRequest{Key: "unknownKey"})
		assert.Contains(t, err.Error(), "404", "Unknown Key results in error: http status code 404, object not found")
	})

	t.Run("Testing Delete & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-delete-key"
		err := s.Init(context.Background(), m)
		require.NoError(t, err)
		// create document.
		err = s.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper key should be errorfree")
		getResponse, _ := s.Get(context.Background(), &state.GetRequest{Key: testKey})
		etag := getResponse.ETag

		incorrectETag := "someRandomETag"
		err = s.Delete(context.Background(), &state.DeleteRequest{Key: testKey, ETag: &incorrectETag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Deleting value with an incorrect etag should be prevented")

		err = s.Delete(context.Background(), &state.DeleteRequest{Key: testKey, ETag: etag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.NoError(t, err, "Deleting value with proper etag should go fine")
	})
}

func testPing(t *testing.T, ociProperties map[string]string) {
	m := state.Metadata{}
	m.Properties = ociProperties
	s := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	t.Run("Ping", func(t *testing.T) {
		err := s.Init(context.Background(), m)
		require.NoError(t, err)
		err = s.Ping(context.Background())
		require.NoError(t, err, "Ping should be successful")
	})
}
