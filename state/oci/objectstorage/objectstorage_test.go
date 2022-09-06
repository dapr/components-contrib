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
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

func getDummyOCIObjectStorageConfiguration() map[string]string {
	return map[string]string{
		"bucketName":      "myBuck",
		"tenancyOCID":     "xxxocid1.tenancy.oc1..aaaaaaaag7c7sq",
		"userOCID":        "xxxxocid1.user.oc1..aaaaaaaaby",
		"compartmentOCID": "xxxocid1.compartment.oc1..aaaaaaaq",
		"fingerPrint":     "xxx02:91:6c",
		"privateKey":      "xxx-----BEGIN RSA PRIVATE KEY-----\nMIIEogI=\n-----END RSA PRIVATE KEY-----",
		"region":          "xxxus-ashburn-1",
	}
}

func TestInit(t *testing.T) {
	meta := state.Metadata{}
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	t.Parallel()
	t.Run("Init with beautifully complete yet incorrect metadata", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		require.Error(t, err, "Incorrect configuration data should result in failure to create client")
		assert.Contains(t, err.Error(), "failed to initialize client", "Incorrect configuration data should result in failure to create client")
	})
	t.Run("Init with missing region", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[regionKey] = ""
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		assert.Equal(t, errors.New("missing or empty region field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing tenancyOCID", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties["tenancyOCID"] = ""
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		assert.Equal(t, errors.New("missing or empty tenancyOCID field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing userOCID", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[userKey] = ""
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		assert.Equal(t, errors.New("missing or empty userOCID field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing compartmentOCID", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[compartmentKey] = ""
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		assert.Equal(t, errors.New("missing or empty compartmentOCID field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing fingerprint", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[fingerPrintKey] = ""
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		assert.Equal(t, errors.New("missing or empty fingerPrint field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with missing private key", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[privateKeyKey] = ""
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err)
		assert.Equal(t, errors.New("missing or empty privateKey field from metadata"), err, "Lacking configuration property should be spotted")
	})
	t.Run("Init with incorrect value for instancePrincipalAuthentication", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[instancePrincipalAuthenticationKey] = "ZQWE"
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err, "if instancePrincipalAuthentication is defined, it should be true or false; if not: error should be raised ")
	})
	t.Run("Init with missing fingerprint with instancePrincipalAuthentication", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[fingerPrintKey] = ""
		meta.Properties[instancePrincipalAuthenticationKey] = "true"
		err := statestore.Init(context.Background(), meta)
		if err != nil {
			assert.Contains(t, err.Error(), "failed to initialize client", "unit tests not run on OCI will not be able to correctly create an OCI client based on instance principal authentication")
		}
	})
	t.Run("Init with configFileAuthentication and incorrect configFilePath", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[configFileAuthenticationKey] = "true"
		meta.Properties[configFilePathKey] = "file_does_not_exist"
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err, "if configFileAuthentication is true and configFilePath does not indicate an existing file, then an error should be produced")
		if err != nil {
			assert.Contains(t, err.Error(), "does not exist", "if configFileAuthentication is true and configFilePath does not indicate an existing file, then an error should be produced that indicates this")
		}
	})
	t.Run("Init with configFileAuthentication and configFilePath starting with ~/", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[configFileAuthenticationKey] = "true"
		meta.Properties[configFilePathKey] = "~/some-file"
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err, "if configFileAuthentication is true and configFilePath contains a value that starts with ~/ , then an error should be produced")
		if err != nil {
			assert.Contains(t, err.Error(), "~", "if configFileAuthentication is true and configFilePath starts with ~/, then an error should be produced that indicates this")
		}
	})
	t.Run("Init with missing fingerprint with false instancePrincipalAuthentication and false configFileAuthentication", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[fingerPrintKey] = ""
		meta.Properties[instancePrincipalAuthenticationKey] = "false"
		err := statestore.Init(context.Background(), meta)
		require.Error(t, err, "if instancePrincipalAuthentication and configFileAuthentication are both false, then fingerprint is required and an error should be raised when it is missing")
	})
	t.Run("Init with missing fingerprint with configFileAuthentication", func(t *testing.T) {
		meta.Properties = getDummyOCIObjectStorageConfiguration()
		meta.Properties[fingerPrintKey] = ""
		meta.Properties[instancePrincipalAuthenticationKey] = "false"
		meta.Properties[configFileAuthenticationKey] = "true"
		err := statestore.Init(context.Background(), meta)
		if err != nil {
			assert.Contains(t, err.Error(), "failed to initialize client", "if configFileAuthentication is true, then fingerprint is not required and error should be raised for failed to initialize client, not for missing fingerprint")
		}
	})
}

func TestFeatures(t *testing.T) {
	t.Parallel()
	s := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	t.Run("Test contents of Features", func(t *testing.T) {
		features := s.Features()
		assert.Contains(t, features, state.FeatureETag)
	})
}

func TestGetObjectStorageMetadata(t *testing.T) {
	t.Parallel()
	t.Run("Test getObjectStorageMetadata with full properties map", func(t *testing.T) {
		meta, err := getObjectStorageMetadata(getDummyOCIObjectStorageConfiguration())
		require.NoError(t, err, "No error expected in clean property set")
		assert.Equal(t, getDummyOCIObjectStorageConfiguration()["region"], meta.Region, "Region in object storage metadata should match region in properties")
	})
	t.Run("Test getObjectStorageMetadata with incomplete property set", func(t *testing.T) {
		properties := map[string]string{
			"region": "xxxus-ashburn-1",
		}
		_, err := getObjectStorageMetadata(properties)
		require.Error(t, err, "Error expected with incomplete property set")
	})
}

type mockedObjectStoreClient struct {
	ociObjectStorageClient
	getIsCalled        bool
	putIsCalled        bool
	deleteIsCalled     bool
	pingBucketIsCalled bool
}

func (c *mockedObjectStoreClient) getObject(ctx context.Context, objectname string) (content []byte, etag *string, metadata map[string]string, err error) {
	c.getIsCalled = true
	etagString := "etag"
	contentString := "Hello World"
	metadata = map[string]string{}

	if objectname == "unknownKey" {
		return nil, nil, nil, nil
	}
	if objectname == "test-expired-ttl-key" {
		metadata[expiryTimeMetaLabel] = time.Now().UTC().Add(time.Second * -10).Format(isoDateTimeFormat)
	}

	if objectname == "test-app/test-key" {
		contentString = "Hello Continent"
	}

	return []byte(contentString), &etagString, metadata, nil
}

func (c *mockedObjectStoreClient) deleteObject(ctx context.Context, objectname string, etag *string) (err error) {
	c.deleteIsCalled = true
	if objectname == "unknownKey" {
		return errors.New("failed to delete object that does not exist - HTTP status code 404")
	}
	if etag != nil && *etag == "notTheCorrectETag" {
		return errors.New("failed to delete object because of incorrect etag-value ")
	}
	return nil
}

func (c *mockedObjectStoreClient) putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string) error {
	c.putIsCalled = true
	if etag != nil && *etag == "notTheCorrectETag" {
		return errors.New("failed to delete object because of incorrect etag-value ")
	}
	if etag != nil && *etag == "correctETag" {
		return nil
	}
	return nil
}

func (c *mockedObjectStoreClient) initStorageBucket(ctx context.Context) error {
	return nil
}

func (c *mockedObjectStoreClient) pingBucket(ctx context.Context) error {
	c.pingBucketIsCalled = true
	return nil
}

func TestGetWithMockClient(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	mockClient := &mockedObjectStoreClient{}
	s.client = mockClient
	t.Parallel()
	t.Run("Test regular Get", func(t *testing.T) {
		getResponse, err := s.Get(context.Background(), &state.GetRequest{Key: "test-key"})
		assert.True(t, mockClient.getIsCalled, "function Get should be invoked on the mockClient")
		assert.Equal(t, "Hello World", string(getResponse.Data), "Value retrieved should be equal to value set")
		assert.NotNil(t, *getResponse.ETag, "ETag should be set")
		require.NoError(t, err)
	})
	t.Run("Test Get with composite key", func(t *testing.T) {
		getResponse, err := s.Get(context.Background(), &state.GetRequest{Key: "test-app||test-key"})
		assert.Equal(t, "Hello Continent", string(getResponse.Data), "Value retrieved should be equal to value set")
		require.NoError(t, err)
	})
	t.Run("Test Get with an unknown key", func(t *testing.T) {
		getResponse, err := s.Get(context.Background(), &state.GetRequest{Key: "unknownKey"})
		assert.Nil(t, getResponse.Data, "No value should be retrieved for an unknown key")
		require.NoError(t, err, "404", "Not finding an object because of unknown key should not result in an error")
	})
	t.Run("Test expired element (because of TTL) ", func(t *testing.T) {
		getResponse, err := s.Get(context.Background(), &state.GetRequest{Key: "test-expired-ttl-key"})
		assert.Nil(t, getResponse.Data, "No value should be retrieved for an expired state element")
		require.NoError(t, err, "Not returning an object because of expiration should not result in an error")
	})
}

func TestInitWithMockClient(t *testing.T) {
	t.Parallel()
	s := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	s.client = &mockedObjectStoreClient{}
	meta := state.Metadata{}
	t.Run("Test Init with incomplete configuration", func(t *testing.T) {
		err := s.Init(context.Background(), meta)
		require.Error(t, err, "Init should complain about lacking configuration settings")
	})
}

func TestPingWithMockClient(t *testing.T) {
	t.Parallel()
	s := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	mockClient := &mockedObjectStoreClient{}
	s.client = mockClient

	t.Run("Test Ping", func(t *testing.T) {
		err := s.Ping(context.Background())
		require.NoError(t, err)
		assert.True(t, mockClient.pingBucketIsCalled, "function pingBucket should be invoked on the mockClient")
	})
}

func TestSetWithMockClient(t *testing.T) {
	t.Parallel()
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	mockClient := &mockedObjectStoreClient{}
	statestore.client = mockClient
	t.Run("Set without a key", func(t *testing.T) {
		err := statestore.Set(context.Background(), &state.SetRequest{Value: []byte("test-value")})
		assert.Equal(t, err, errors.New("key for value to set was missing from request"), "Lacking Key results in error")
	})
	t.Run("Regular Set Operation", func(t *testing.T) {
		testKey := "test-key"
		err := statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value")})
		require.NoError(t, err, "Setting a value with a proper key should be errorfree")
		assert.True(t, mockClient.putIsCalled, "function put should be invoked on the mockClient")
	})
	t.Run("Regular Set Operation with TTL", func(t *testing.T) {
		testKey := "test-key"
		err := statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "5",
		})})
		require.NoError(t, err, "Setting a value with a proper key and a correct TTL value should be errorfree")

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "XXX",
		})})
		require.Error(t, err, "Setting a value with a proper key and a incorrect TTL value should be produce an error")

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("test-value"), Metadata: (map[string]string{
			"ttlInSeconds": "1",
		})})
		require.NoError(t, err, "Setting a value with a proper key and a correct TTL value should be errorfree")
	})
	t.Run("Testing Set & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-key"
		incorrectETag := "notTheCorrectETag"
		etag := "correctETag"

		err := statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: &incorrectETag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Updating value with wrong etag should fail")

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: nil, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Asking for FirstWrite concurrency policy without ETag should fail")

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: &etag, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.NoError(t, err, "Updating value with proper etag should go fine")

		err = statestore.Set(context.Background(), &state.SetRequest{Key: testKey, Value: []byte("overwritten-value"), ETag: nil, Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Updating value with concurrency policy at FirstWrite should fail when ETag is missing")
	})
}

func TestDeleteWithMockClient(t *testing.T) {
	t.Parallel()
	s := NewOCIObjectStorageStore(logger.NewLogger("logger")).(*StateStore)
	mockClient := &mockedObjectStoreClient{}
	s.client = mockClient
	t.Run("Delete without a key", func(t *testing.T) {
		err := s.Delete(context.Background(), &state.DeleteRequest{})
		assert.Equal(t, err, errors.New("key for value to delete was missing from request"), "Lacking Key results in error")
	})
	t.Run("Delete with an unknown key", func(t *testing.T) {
		err := s.Delete(context.Background(), &state.DeleteRequest{Key: "unknownKey"})
		assert.Contains(t, err.Error(), "404", "Unknown Key results in error: http status code 404, object not found")
	})
	t.Run("Regular Delete Operation", func(t *testing.T) {
		testKey := "test-key"
		err := s.Delete(context.Background(), &state.DeleteRequest{Key: testKey})
		require.NoError(t, err, "Deleting an existing value with a proper key should be errorfree")
		assert.True(t, mockClient.deleteIsCalled, "function delete should be invoked on the mockClient")
	})
	t.Run("Testing Delete & Concurrency (ETags)", func(t *testing.T) {
		testKey := "etag-test-delete-key"
		incorrectETag := "notTheCorrectETag"
		err := s.Delete(context.Background(), &state.DeleteRequest{Key: testKey, ETag: &incorrectETag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Deleting value with an incorrect etag should be prevented")

		etag := "correctETag"
		err = s.Delete(context.Background(), &state.DeleteRequest{Key: testKey, ETag: &etag, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.NoError(t, err, "Deleting value with proper etag should go fine")

		err = s.Delete(context.Background(), &state.DeleteRequest{Key: testKey, ETag: nil, Options: state.DeleteStateOption{
			Concurrency: state.FirstWrite,
		}})
		require.Error(t, err, "Asking for FirstWrite concurrency policy without ETag should fail")
	})
}

func TestGetFilename(t *testing.T) {
	t.Parallel()
	t.Run("Valid composite key", func(t *testing.T) {
		filename := getFileName("app-id||key")
		assert.Equal(t, "app-id/key", filename)
	})

	t.Run("Normal (not-composite) key", func(t *testing.T) {
		filename := getFileName("app-id-key")
		assert.Equal(t, "app-id-key", filename)
	})
}
