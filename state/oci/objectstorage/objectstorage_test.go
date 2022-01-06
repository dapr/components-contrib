/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func getDummyOCIObjectStorageConfiguration() map[string]string {
	return map[string]string{
		"bucketName":      "myBuck",
		"tenancyOCID":     "ocid1.tenancy.oc1..aaaaaaaag7c7sq",
		"userOCID":        "ocid1.user.oc1..aaaaaaaaby",
		"compartmentOCID": "ocid1.compartment.oc1..aaaaaaaq",
		"fingerPrint":     "02:91:6c",
		"privateKey":      "-----BEGIN RSA PRIVATE KEY-----\nMIIEogI=\n-----END RSA PRIVATE KEY-----",
		"region":          "us-ashburn-1",
	}
}

func TestInit(t *testing.T) {
	meta := state.Metadata{}
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Init with complete yet incorrect metadata", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Error(t, err, "Incorrect configuration data should result in failure to create client")
		assert.Contains(t, err.Error(), "failed to initialize client", "Incorrect configuration data should result in failure to create client")
	})
	t.Run("Init with missing region", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[regionKey] = ""
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty region field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing tenancyOCID", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties["tenancyOCID"] = ""
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty tenancyOCID field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing userOCID", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[userKey] = ""
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty userOCID field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing compartmentOCID", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[compartmentKey] = ""
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty compartmentOCID field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing fingerprint", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[fingerPrintKey] = ""
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty fingerPrint field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing private key", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[privateKeyKey] = ""
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty privateKey field from metadata"), err, "Lacking configuration property should be spotted")
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
	ociObjectStorageClient
	getIsCalled        bool
	putIsCalled        bool
	deleteIsCalled     bool
	pingBucketIsCalled bool
}

func (c *mockedObjectStoreClient) getObject(ctx context.Context, objectname string, logger logger.Logger) (content []byte, etag *string, metadata map[string]string, err error) {
	c.getIsCalled = true
	etagString := "etag"
	contentString := "Hello World"
	metadata = map[string]string{}

	if objectname == "unknownKey" {
		return nil, nil, nil, nil
	}

	return []byte(contentString), &etagString, metadata, nil
}

func (c *mockedObjectStoreClient) deleteObject(ctx context.Context, objectname string, etag *string) (err error) {
	c.deleteIsCalled = true
	if objectname == "unknownKey" {
		return fmt.Errorf("failed to delete object that does not exist - HTTP status code 404")
	}
	if etag != nil && *etag == "notTheCorrectETag" {
		return fmt.Errorf("failed to delete object because of incorrect etag-value ")
	}
	return nil
}

func (c *mockedObjectStoreClient) putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string, logger logger.Logger) error {
	c.putIsCalled = true
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
	c.pingBucketIsCalled = true
	return nil
}

func TestGetWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	mockClient := &mockedObjectStoreClient{}
	s.client = mockClient

	t.Run("Test regular Get", func(t *testing.T) {
		getResponse, err := s.Get(&state.GetRequest{Key: "test-key"})
		assert.True(t, mockClient.getIsCalled, "function Get should be invoked on the mockClient")
		assert.Equal(t, "Hello World", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
		assert.Nil(t, err)
	})
	t.Run("Test Get with an unknown key", func(t *testing.T) {
		getResponse, err := s.Get(&state.GetRequest{Key: "unknownKey"})
		assert.Nil(t, getResponse.Data, "No value should be retrieved for an unknown key")
		assert.Nil(t, err, "404", "Not finding an object because of unknown key should not result in an error")
	})
}

func TestInitWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	s.client = &mockedObjectStoreClient{}
	meta := state.Metadata{}
	t.Run("Test Init with incomplete configuration", func(t *testing.T) {
		err := s.Init(meta)
		assert.NotNil(t, err, "Init should complain about lacking configuration settings")
	})
}

func TestPingWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	mockClient := &mockedObjectStoreClient{}
	s.client = mockClient

	t.Run("Test Ping", func(t *testing.T) {
		err := s.Ping()
		assert.Nil(t, err)
		assert.True(t, mockClient.pingBucketIsCalled, "function pingBucket should be invoked on the mockClient")
	})
}

func TestSetWithMockClient(t *testing.T) {
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	mockClient := &mockedObjectStoreClient{}
	statestore.client = mockClient
	t.Run("Set without a key", func(t *testing.T) {
		err := statestore.Set(&state.SetRequest{Value: []byte("test-value")})
		assert.Equal(t, err, fmt.Errorf("key for value to set was missing from request"), "Lacking Key results in error")
	})
	t.Run("Regular Set Operation", func(t *testing.T) {
		testKey := "test-key"
		err := statestore.Set(&state.SetRequest{Key: testKey, Value: []byte("test-value")})
		assert.Nil(t, err, "Setting a value with a proper key should be errorfree")
		assert.True(t, mockClient.putIsCalled, "function put should be invoked on the mockClient")
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
	mockClient := &mockedObjectStoreClient{}
	s.client = mockClient
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
		assert.True(t, mockClient.deleteIsCalled, "function delete should be invoked on the mockClient")
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
