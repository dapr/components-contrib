package objectstorage

// for example in ~/dapr-dev/components-contrib
// go test -v github.com/dapr/components-contrib/state/oci/objectstorage
// to run the test set for OCI ObjectStorage service based StateStore.

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

var dummyOCIObjectStorageConfiguration = map[string]string{
	"bucketName":      "myBuck",
	"tenancyOCID":     "ocid1.tenancy.oc1..aaaaaaaag7c7sq",
	"userOCID":        "ocid1.user.oc1..aaaaaaaaby",
	"compartmentOCID": "ocid1.compartment.oc1..aaaaaaaq",
	"fingerPrint":     "02:91:6c",
	"privateKey":      "-----BEGIN RSA PRIVATE KEY-----\nMIIEogI=\n-----END RSA PRIVATE KEY-----",
	"region":          "us-ashburn-1",
}

func TestInit(t *testing.T) {
	meta := state.Metadata{}
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Init with missing metadata", func(t *testing.T) {
		meta.Properties = map[string]string{
			"invalidValue": "a",
		}
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty bucketName field from metadata"), err, "Lacking configuration property should be spotted")
	})

	t.Run("Init with complete yet incorrect metadata", func(t *testing.T) {
		meta.Properties = dummyOCIObjectStorageConfiguration
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Error(t, err, "Incorrect configuration data should result in failure to create client")
		assert.Contains(t, err.Error(), "failed to initialize client", "Incorrect configuration data should result in failure to create client")
	})
}

func TestFeatures(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Test contents of Features", func(t *testing.T) {
		features := s.Features()
		assert.Contains(t, features, state.FeatureETag)
	})
}

type mockedObjectStoreClient struct {
	objectStoreClient
}

func (c *mockedObjectStoreClient) getObject(ctx context.Context, objectname string, logger logger.Logger) ([]byte, *string, error) {
	etag := "etag"
	return []byte("Hello World"), &etag, nil
}

func (c *mockedObjectStoreClient) deleteObject(ctx context.Context, objectname string, etag *string) (err error) {
	if objectname == "unknownKey" {
		return fmt.Errorf("failed to delete object that does not exist - HTTP status code 404")
	}
	if etag != nil && *etag == "notTheCorrectETag" {
		return fmt.Errorf("failed to delete object because of incorrect etag-value ")
	}
	return nil
}

func (c *mockedObjectStoreClient) putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string, logger logger.Logger) error {
	if etag != nil && *etag == "notTheCorrectETag" {
		return fmt.Errorf("failed to delete object because of incorrect etag-value ")
	}
	if etag != nil && *etag == "correctETag" {
		return nil
	}
	return nil
}

func (c *mockedObjectStoreClient) initStorageBucket(logger logger.Logger) error {
	return nil
}

func (c *mockedObjectStoreClient) pingBucket(logger logger.Logger) error {
	return nil
}

func TestGetWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	s.client = &mockedObjectStoreClient{}

	t.Run("Test contents of Features", func(t *testing.T) {
		getResponse, err := s.Get(&state.GetRequest{Key: "test-key"})
		assert.Equal(t, "Hello World", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
		assert.Nil(t, err)
	})
}

func TestInitWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	s.client = &mockedObjectStoreClient{}

	t.Run("Test Init", func(t *testing.T) {
		getResponse, err := s.Get(&state.GetRequest{Key: "test-key"})
		assert.Equal(t, "Hello World", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
		assert.Nil(t, err)
	})
}

func TestPingWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	s.client = &mockedObjectStoreClient{}

	t.Run("Test Ping", func(t *testing.T) {
		err := s.Ping()
		assert.Nil(t, err)
	})
}
func TestSetWithMockClient(t *testing.T) {
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	statestore.client = &mockedObjectStoreClient{}
	t.Run("Set without a key", func(t *testing.T) {
		err := statestore.Set(&state.SetRequest{Value: []byte("test-value")})
		assert.Equal(t, err, fmt.Errorf("key for value to set was missing from request"), "Lacking Key results in error")
	})
	t.Run("Regular Set Operation", func(t *testing.T) {
		testKey := "test-key"
		err := statestore.Set(&state.SetRequest{Key: testKey, Value: []byte("test-value")})
		assert.Nil(t, err, "Setting a value with a proper key should be errorfree")
	})
	t.Run("Testing Set & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-key"
		incorrectETag := "notTheCorrectETag"
		etag := "correctETag"

		err := statestore.Set(&state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: &incorrectETag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Updating value with wrong etag should fail")

		err = statestore.Set(&state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: nil, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Asking for FirstWrite concurrency policy without ETag should fail")

		err = statestore.Set(&state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: &etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.Nil(t, err, "Updating value with proper etag should go fine")

		err = statestore.Set(&state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: nil, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Updating value with concurrency policy at FirstWrite should fail when ETag is missing")
	})
}

func TestDeleteWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	s.client = &mockedObjectStoreClient{}
	t.Run("Delete without a key", func(t *testing.T) {
		err := s.Delete(&state.DeleteRequest{})
		assert.Equal(t, err, fmt.Errorf("key for value to delete was missing from request"), "Lacking Key results in error")
	})
	t.Run("Delete with an unknown key", func(t *testing.T) {
		err := s.Delete(&state.DeleteRequest{Key: "unknownKey"})
		assert.Contains(t, err.Error(), "404", "Unknown Key results in error: http status code 404, object not found")
	})
	t.Run("Regular Delete Operation", func(t *testing.T) {
		testKey := "test-key"
		err := s.Delete(&state.DeleteRequest{Key: testKey})
		assert.Nil(t, err, "Deleting an existing value with a proper key should be errorfree")
	})
	t.Run("Testing Delete & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-delete-key"
		incorrectETag := "notTheCorrectETag"
		err := s.Delete(&state.DeleteRequest{Key: testKey, ETag: &incorrectETag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Deleting value with an incorrect etag should be prevented")

		etag := "correctETag"
		err = s.Delete(&state.DeleteRequest{Key: testKey, ETag: &etag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.Nil(t, err, "Deleting value with proper etag should go fine")

		err = s.Delete(&state.DeleteRequest{Key: testKey, ETag: nil, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Asking for FirstWrite concurrency policy without ETag should fail")

	})
}
