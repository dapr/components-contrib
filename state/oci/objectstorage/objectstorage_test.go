package objectstorage

/*
for example in ~/dapr-dev/components-contrib
go test -v github.com/dapr/components-contrib/state/oci/objectstorage

*/

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	namespace = "idtwlqf2hanz"
)

var ociObjectStorageConfiguration = map[string]string{
	"bucketName":      "myBuck",
	"tenancyOCID":     "ocid1.tenancy.oc1..aaaaaaaag7c7sq",
	"userOCID":        "ocid1.user.oc1..aaaaaaaaby4oyytselv5q",
	"compartmentOCID": "ocid1.compartment.oc1..aaaaaaaq",
	"fingerPrint":     "02:91:6c:49:d8:04:56",
	"privateKey":      "-----BEGIN RSA PRIVATE KEY-----\nMIIEogI=\n-----END RSA PRIVATE KEY-----",
	"region":          "us-ashburn-1",
}

func TestInit(t *testing.T) {
	m := state.Metadata{}
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	//s.logger.SetOutputLevel(logger.DebugLevel)
	t.Run("Init with valid metadata", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		assert.Equal(t, "myBuck", s.objectStorageMetadata.bucketName)
		assert.Equal(t, namespace, s.objectStorageMetadata.namespace)
	})

	t.Run("Init with missing metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"invalidValue": "a",
		}
		err := s.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing or empty bucketName field from metadata"), "Lacking configuration property should be spotted")
	})
}

func TestGet(t *testing.T) {
	m := state.Metadata{}
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Get an non-existing key", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		getResponse, err := s.Get(&state.GetRequest{Key: "xyzq"})
		assert.Equal(t, &state.GetResponse{}, getResponse, "Response must be empty")
		assert.NoError(t, err, "Non-existing key must not be treated as error")
	})
	t.Run("Get an existing key", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		err = s.Set(&state.SetRequest{Key: "test-key", Value: []byte("test-value")})
		getResponse, err := s.Get(&state.GetRequest{Key: "test-key"})
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
	})
}

func TestSet(t *testing.T) {
	m := state.Metadata{}
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Set without a key", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		err = s.Set(&state.SetRequest{Value: []byte("test-value")})
		assert.Equal(t, err, fmt.Errorf("key for value to set was missing from request"), "Lacking Key results in error")

		// getResponse, err := s.Get(&state.GetRequest{Key: "test-key"})
		// assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
		// assert.NotNil(t, *getResponse.ETag, "ETag should be set")
	})
	t.Run("Regular Set Operation", func(t *testing.T) {
		testKey := "test-key"
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)

		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("test-value")})
		assert.Nil(t, err, "Setting a value with a proper key should be errorfree")
		getResponse, err := s.Get(&state.GetRequest{Key: testKey})
		assert.Equal(t, "test-value", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")

	})
	t.Run("Testing Set & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-key"
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)

		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("test-value")})
		assert.Nil(t, err, "Setting a value with a proper key should be errorfree")
		getResponse, err := s.Get(&state.GetRequest{Key: testKey})
		etag := getResponse.ETag
		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.Nil(t, err, "Updating value with proper etag should go fine")
		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("more-overwritten-value"), ETag: etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Updating value with the old etag should be refused")

		// retrieve the latest etag - assigned by the previous set operation
		getResponse, err = s.Get(&state.GetRequest{Key: testKey})
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
		etag = getResponse.ETag
		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("more-overwritten-value"), ETag: etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.Nil(t, err, "Updating value with the latest etag should be accepted")

	})
}

func TestDelete(t *testing.T) {
	m := state.Metadata{}
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Delete without a key", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		err = s.Delete(&state.DeleteRequest{})
		assert.Equal(t, err, fmt.Errorf("key for value to delete was missing from request"), "Lacking Key results in error")

	})
	t.Run("Regular Delete Operation", func(t *testing.T) {
		testKey := "test-key"
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)

		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("test-value")})
		assert.Nil(t, err, "Setting a value with a proper key should be errorfree")
		err = s.Delete(&state.DeleteRequest{Key: testKey})
		assert.Nil(t, err, "Deleting an existing value with a proper key should be errorfree")
	})
	t.Run("Testing Delete & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-delete-key"
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		// create document
		err = s.Set(&state.SetRequest{Key: testKey, Value: []byte("test-value")})
		assert.Nil(t, err, "Setting a value with a proper key should be errorfree")
		getResponse, err := s.Get(&state.GetRequest{Key: testKey})
		etag := getResponse.ETag

		incorrectETag := "notTheCorrectETag"
		err = s.Delete(&state.DeleteRequest{Key: testKey, ETag: &incorrectETag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.NotNil(t, err, "Deleting value with an incorrect etag should be prevented")

		err = s.Delete(&state.DeleteRequest{Key: testKey, ETag: etag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		assert.Nil(t, err, "Deleting value with proper etag should go fine")

	})
}

func TestFeatures(t *testing.T) {
	m := state.Metadata{}
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Test contents of Features", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		features := s.Features()
		assert.Contains(t, features, state.FeatureETag)
	})
}

func TestPing(t *testing.T) {
	m := state.Metadata{}
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Ping", func(t *testing.T) {
		m.Properties = ociObjectStorageConfiguration
		err := s.Init(m)
		assert.Nil(t, err)
		err = s.Ping()
		assert.Nil(t, err, "Ping should be successful")

	})
}
