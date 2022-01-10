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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/oracle/oci-go-sdk/v54/common"
	"github.com/oracle/oci-go-sdk/v54/common/auth"
	"github.com/oracle/oci-go-sdk/v54/objectstorage"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	keyDelimiter                       = "||"
	instancePrincipalAuthenticationKey = "instancePrincipalAuthentication"
	configFileAuthenticationKey        = "configFileAuthentication"
	configFilePathKey                  = "configFilePath"
	configFileProfileKey               = "configFileProfile"
	tenancyKey                         = "tenancyOCID"
	compartmentKey                     = "compartmentOCID"
	regionKey                          = "region"
	fingerPrintKey                     = "fingerPrint"
	privateKeyKey                      = "privateKey"
	userKey                            = "userOCID"
	bucketNameKey                      = "bucketName"
	metadataTTLKey                     = "ttlInSeconds"
	daprStateStoreMetaLabel            = "dapr-state-store"
	expiryTimeMetaLabel                = "expiry-time-from-ttl"
	isoDateTimeFormat                  = "2006-01-02T15:04:05"
)

type StateStore struct {
	state.DefaultBulkStore

	json     jsoniter.API
	features []state.Feature
	logger   logger.Logger
	client   objectStoreClient
}

type Metadata struct {
	userOCID                        string
	bucketName                      string
	region                          string
	tenancyOCID                     string
	fingerPrint                     string
	privateKey                      string
	compartmentOCID                 string
	namespace                       string
	configFilePath                  string
	configFileProfile               string
	instancePrincipalAuthentication bool
	configFileAuthentication        bool

	OCIObjectStorageClient *objectstorage.ObjectStorageClient
}

type objectStoreClient interface {
	getObject(ctx context.Context, objectname string, logger logger.Logger) (content []byte, etag *string, metadata map[string]string, err error)
	deleteObject(ctx context.Context, objectname string, etag *string) (err error)
	putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string, logger logger.Logger) error
	initStorageBucket(logger logger.Logger) error
	initOCIObjectStorageClient(logger logger.Logger) (*objectstorage.ObjectStorageClient, error)
	pingBucket(logger logger.Logger) error
}

type objectStorageClient struct {
	objectStorageMetadata *Metadata
}

type ociObjectStorageClient struct {
	objectStorageClient
}

/*********  Interface Implementations Init, Features, Get, Set, Delete and the instantiation function NewOCIObjectStorageStore. */

func (r *StateStore) Init(metadata state.Metadata) error {
	r.logger.Debugf("Init OCI Object Storage State Store")
	meta, err := getObjectStorageMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	r.client = &ociObjectStorageClient{objectStorageClient{objectStorageMetadata: meta}}

	objectStorageClient, cerr := r.client.initOCIObjectStorageClient(r.logger)
	if cerr != nil {
		return fmt.Errorf("failed to initialize client or create bucket : %w", cerr)
	}
	meta.OCIObjectStorageClient = objectStorageClient

	cerr = r.client.initStorageBucket(r.logger)
	if cerr != nil {
		return fmt.Errorf("failed to create bucket : %w", cerr)
	}
	r.logger.Debugf("OCI Object Storage State Store initialized using bucket '%s'", meta.bucketName)

	return nil
}

func (r *StateStore) Features() []state.Feature {
	return r.features
}

func (r *StateStore) Delete(req *state.DeleteRequest) error {
	r.logger.Debugf("Delete entry from OCI Object Storage State Store with key ", req.Key)
	err := r.deleteDocument(req)
	return err
}

func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("Get from OCI Object Storage State Store with key ", req.Key)
	content, etag, err := r.readDocument((req))
	if err != nil {
		r.logger.Debugf("error %s", err)
		if err.Error() == "ObjectNotFound" {
			return &state.GetResponse{Data: nil, ETag: nil, Metadata: nil}, nil
		}

		return &state.GetResponse{Data: nil, ETag: nil, Metadata: nil}, err
	}
	return &state.GetResponse{
		Data:     content,
		ETag:     etag,
		Metadata: nil,
	}, err
}

func (r *StateStore) Set(req *state.SetRequest) error {
	r.logger.Debugf("saving %s to OCI Object Storage State Store", req.Key)
	return r.writeDocument(req)
}

func (r *StateStore) Ping() error {
	return r.pingBucket()
}

func NewOCIObjectStorageStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
		client:   nil,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

/************** private helper functions. */

func getObjectStorageMetadata(metadata map[string]string) (*Metadata, error) {
	meta := Metadata{}

	instancePrincipalAuthenticationString, _ := getValue(metadata, instancePrincipalAuthenticationKey, false)
	if instancePrincipalAuthenticationString == "" {
		instancePrincipalAuthenticationString = "false"
	}
	instancePrincipalAuthentication, err := strconv.ParseBool(instancePrincipalAuthenticationString)
	if err != nil {
		return nil, fmt.Errorf("incorrect value %s for %s, should be 'true' or 'false'", instancePrincipalAuthenticationString, instancePrincipalAuthenticationKey)
	}
	meta.instancePrincipalAuthentication = instancePrincipalAuthentication

	configFileAuthenticationString, _ := getValue(metadata, configFileAuthenticationKey, false)
	if configFileAuthenticationString == "" {
		configFileAuthenticationString = "false"
	}
	configFileAuthentication, err := strconv.ParseBool(configFileAuthenticationString)
	if err != nil {
		return nil, fmt.Errorf("incorrect value %s for %s, should be 'true' or 'false'", configFileAuthenticationString, configFileAuthenticationKey)
	}
	meta.configFileAuthentication = configFileAuthentication

	meta.configFilePath, _ = getValue(metadata, configFilePathKey, false)
	if meta.configFilePath, _ = getValue(metadata, configFilePathKey, false); meta.configFilePath != "" {
		if _, err := os.Stat(meta.configFilePath); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("oci configuration file %s does not exist %w", meta.configFilePath, err)
			}
			return nil, fmt.Errorf("error  %w with reading oci configuration file %s", err, meta.configFilePath)
		}
	}
	meta.configFileProfile, _ = getValue(metadata, configFileProfileKey, false)

	externalAuthentication := instancePrincipalAuthentication || configFileAuthentication
	if meta.bucketName, err = getValue(metadata, bucketNameKey, true); err != nil {
		return nil, err
	}
	if meta.region, err = getValue(metadata, regionKey, !externalAuthentication); err != nil {
		return nil, err
	}
	if meta.compartmentOCID, err = getValue(metadata, compartmentKey, true); err != nil {
		return nil, err
	}
	if meta.userOCID, err = getValue(metadata, userKey, !externalAuthentication); err != nil {
		return nil, err
	}
	if meta.fingerPrint, err = getValue(metadata, fingerPrintKey, !externalAuthentication); err != nil {
		return nil, err
	}
	if meta.privateKey, err = getValue(metadata, privateKeyKey, !externalAuthentication); err != nil {
		return nil, err
	}
	if meta.tenancyOCID, err = getValue(metadata, tenancyKey, !externalAuthentication); err != nil {
		return nil, err
	}
	return &meta, nil
}

func getValue(metadata map[string]string, key string, valueRequired bool) (value string, err error) {
	if val, ok := metadata[key]; ok && val != "" {
		return val, nil
	}
	if !valueRequired {
		return "", nil
	}
	return "", fmt.Errorf("missing or empty %s field from metadata", key)
}

// functions that bridge from the Dapr State API to the OCI ObjectStorage Client.
func (r *StateStore) writeDocument(req *state.SetRequest) error {
	if len(req.Key) == 0 || req.Key == "" {
		return fmt.Errorf("key for value to set was missing from request")
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || len(*req.ETag) == 0) {
		r.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return fmt.Errorf("when FirstWrite is to be enforced, a value must be provided for the ETag")
	}
	metadata := (map[string]string{"category": daprStateStoreMetaLabel})

	err := convertTTLtoExpiryTime(req, r.logger, metadata)
	if err != nil {
		return fmt.Errorf("failed to process ttl meta data: %w", err)
	}

	r.logger.Debugf("Save state in OCI Object Storage Bucket under key %s ", req.Key)
	objectName := getFileName(req.Key)
	content := r.marshal(req)
	objectLength := int64(len(content))
	ctx := context.Background()
	etag := req.ETag
	if req.Options.Concurrency != state.FirstWrite {
		etag = nil
	}
	err = r.client.putObject(ctx, objectName, objectLength, ioutil.NopCloser(bytes.NewReader(content)), metadata, etag, r.logger)
	if err != nil {
		r.logger.Debugf("error in writing object to OCI object storage  %s, err %s", req.Key, err)
		return fmt.Errorf("failed to write object to OCI Object storage : %w", err)
	}
	return nil
}

func convertTTLtoExpiryTime(req *state.SetRequest, logger logger.Logger, metadata map[string]string) error {
	ttl, ttlerr := parseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error in parsing TTL %w", ttlerr)
	}
	if ttl != nil {
		if *ttl == -1 {
			logger.Debugf("TTL is set to -1; this means: never expire. ")
		} else {
			metadata[expiryTimeMetaLabel] = time.Now().UTC().Add(time.Second * time.Duration(*ttl)).Format(isoDateTimeFormat)
			logger.Debugf("Set %s in meta properties for object to ", expiryTimeMetaLabel, metadata[expiryTimeMetaLabel])
		}
	}
	return nil
}

func (r *StateStore) readDocument(req *state.GetRequest) ([]byte, *string, error) {
	if len(req.Key) == 0 || req.Key == "" {
		return nil, nil, fmt.Errorf("key for value to get was missing from request")
	}
	objectName := getFileName(req.Key)
	ctx := context.Background()
	content, etag, meta, err := r.client.getObject(ctx, objectName, r.logger)
	if err != nil {
		r.logger.Debugf("download file %s, err %s", req.Key, err)
		return nil, nil, fmt.Errorf("failed to read object from OCI Object storage : %w", err)
	}
	if expiryTimeString, ok := meta[expiryTimeMetaLabel]; ok {
		expirationTime, err := time.Parse(isoDateTimeFormat, expiryTimeString)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get object from OCI because of invalid formatted value %s in meta property %s  : %w", expiryTimeString, expiryTimeMetaLabel, err)
		}
		if time.Now().UTC().After(expirationTime) {
			r.logger.Debug("failed to get object from OCI because it has expired; expiry time set to %s", expiryTimeString)
			return nil, nil, nil
		}
	}
	return content, etag, nil
}

func (r *StateStore) pingBucket() error {
	err := r.client.pingBucket(r.logger)
	if err != nil {
		r.logger.Debugf("ping bucket failed err %s", err)
		return fmt.Errorf("failed to ping bucket on OCI Object storage : %w", err)
	}
	return nil
}

func (r *StateStore) deleteDocument(req *state.DeleteRequest) error {
	if len(req.Key) == 0 || req.Key == "" {
		return fmt.Errorf("key for value to delete was missing from request")
	}

	objectName := getFileName(req.Key)
	ctx := context.Background()
	etag := req.ETag
	if req.Options.Concurrency != state.FirstWrite {
		etag = nil
	}
	if req.Options.Concurrency == state.FirstWrite && (etag == nil || len(*etag) == 0) {
		r.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return fmt.Errorf("when FirstWrite is to be enforced, a value must be provided for the ETag")
	}
	err := r.client.deleteObject(ctx, objectName, etag)
	if err != nil {
		r.logger.Debugf("error in deleting object from OCI object storage  %s, err %s", req.Key, err)
		return fmt.Errorf("failed to delete object from OCI Object storage : %w", err)
	}
	return nil
}

func (r *StateStore) marshal(req *state.SetRequest) []byte {
	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}
	return []byte(v)
}

func getFileName(key string) string {
	pr := strings.Split(key, keyDelimiter)
	if len(pr) != 2 {
		return pr[0]
	}

	return path.Join(pr[0], pr[1])
}

func parseTTL(requestMetadata map[string]string) (*int, error) {
	if val, found := requestMetadata[metadataTTLKey]; found && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("error in parsing ttl metadata : %w", err)
		}
		parsedInt := int(parsedVal)

		return &parsedInt, nil
	}

	return nil, nil
}

/**************** functions with OCI ObjectStorage Service interaction.   */

func getNamespace(ctx context.Context, client objectstorage.ObjectStorageClient) (string, error) {
	request := objectstorage.GetNamespaceRequest{}
	response, err := client.GetNamespace(ctx, request)
	if err != nil {
		return *response.Value, fmt.Errorf("failed to retrieve tenancy namespace : %w", err)
	}

	return *response.Value, nil
}

// bucketname needs to be unique within compartment. there is no concept of "child" buckets.
// the value returned is the bucket's OCID.
func ensureBucketExists(ctx context.Context, client objectstorage.ObjectStorageClient, namespace string, name string, compartmentOCID string, logger logger.Logger) error {
	req := objectstorage.GetBucketRequest{
		NamespaceName: &namespace,
		BucketName:    &name,
	}
	// verify if bucket exists.
	response, err := client.GetBucket(ctx, req)
	if err != nil {
		if response.RawResponse.StatusCode == 404 {
			err = createBucket(ctx, client, namespace, name, compartmentOCID)
			if err == nil {
				logger.Debugf("Created OCI Object Storage Bucket %s as State Store", name)
			}
			return err
		}
		return err
	}
	return nil
}

// bucketname needs to be unique within compartment. there is no concept of "child" buckets.
func createBucket(ctx context.Context, client objectstorage.ObjectStorageClient, namespace string, name string, compartmentOCID string) error {
	request := objectstorage.CreateBucketRequest{
		NamespaceName: &namespace,
	}
	request.CompartmentId = &compartmentOCID
	request.Name = &name
	request.Metadata = make(map[string]string)
	request.PublicAccessType = objectstorage.CreateBucketDetailsPublicAccessTypeNopublicaccess
	_, err := client.CreateBucket(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to create bucket on OCI : %w", err)
	}
	return nil
}

// *****  the functions that interact with OCI Object Storage AND constitute the objectStoreClient interface.

func (c *ociObjectStorageClient) getObject(ctx context.Context, objectname string, logger logger.Logger) (content []byte, etag *string, metadata map[string]string, err error) {
	logger.Debugf("read file %s from OCI ObjectStorage StateStore %s ", objectname, &c.objectStorageMetadata.bucketName)
	request := objectstorage.GetObjectRequest{
		NamespaceName: &c.objectStorageMetadata.namespace,
		BucketName:    &c.objectStorageMetadata.bucketName,
		ObjectName:    &objectname,
	}
	response, err := c.objectStorageMetadata.OCIObjectStorageClient.GetObject(ctx, request)
	if err != nil {
		logger.Debugf("Issue in OCI ObjectStorage with retrieving object %s, error:  %s", objectname, err)
		if response.RawResponse.StatusCode == 404 {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("failed to retrieve object : %w", err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(response.Content)
	return buf.Bytes(), response.ETag, response.OpcMeta, nil
}

func (c *ociObjectStorageClient) deleteObject(ctx context.Context, objectname string, etag *string) (err error) {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName: &c.objectStorageMetadata.namespace,
		BucketName:    &c.objectStorageMetadata.bucketName,
		ObjectName:    &objectname,
		IfMatch:       etag,
	}
	_, err = c.objectStorageMetadata.OCIObjectStorageClient.DeleteObject(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to delete object from OCI : %w", err)
	}
	return nil
}

func (c *ociObjectStorageClient) putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string, logger logger.Logger) error {
	request := objectstorage.PutObjectRequest{
		NamespaceName: &c.objectStorageMetadata.namespace,
		BucketName:    &c.objectStorageMetadata.bucketName,
		ObjectName:    &objectname,
		ContentLength: &contentLen,
		PutObjectBody: content,
		OpcMeta:       metadata,
		IfMatch:       etag,
	}
	_, err := c.objectStorageMetadata.OCIObjectStorageClient.PutObject(ctx, request)
	logger.Debugf("Put object ", objectname, " in bucket ", &c.objectStorageMetadata.bucketName)
	if err != nil {
		return fmt.Errorf("failed to put object on OCI : %w", err)
	}
	return nil
}

func (c *ociObjectStorageClient) initStorageBucket(logger logger.Logger) error {
	ctx := context.Background()
	err := ensureBucketExists(ctx, *c.objectStorageMetadata.OCIObjectStorageClient, c.objectStorageMetadata.namespace, c.objectStorageMetadata.bucketName, c.objectStorageMetadata.compartmentOCID, logger)
	if err != nil {
		return fmt.Errorf("failed to read or create bucket : %w", err)
	}
	return nil
}

func (c *ociObjectStorageClient) initOCIObjectStorageClient(logger logger.Logger) (*objectstorage.ObjectStorageClient, error) {
	var configurationProvider common.ConfigurationProvider
	if c.objectStorageMetadata.instancePrincipalAuthentication {
		logger.Debugf("instance principal authentication is used. ")
		var err error
		configurationProvider, err = auth.InstancePrincipalConfigurationProvider()
		if err != nil {
			return nil, fmt.Errorf("failed to get oci configurationprovider based on instance principal authentication : %w", err)
		}
	} else {
		if c.objectStorageMetadata.configFileAuthentication {
			logger.Debugf("configuration file based authentication is used with configuration file path %s and configuration profile %s. ", c.objectStorageMetadata.configFilePath, c.objectStorageMetadata.configFileProfile)
			configurationProvider = common.CustomProfileConfigProvider(c.objectStorageMetadata.configFilePath, c.objectStorageMetadata.configFileProfile)
		} else {
			logger.Debugf("identity authentication is used with configuration provided through Dapr component configuration ")
			configurationProvider = common.NewRawConfigurationProvider(c.objectStorageMetadata.tenancyOCID, c.objectStorageMetadata.userOCID, c.objectStorageMetadata.region, c.objectStorageMetadata.fingerPrint, c.objectStorageMetadata.privateKey, nil)
		}
	}

	objectStorageClient, cerr := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	if cerr != nil {
		return nil, fmt.Errorf("failed to create ObjectStorageClient : %w", cerr)
	}
	ctx := context.Background()
	c.objectStorageMetadata.namespace, cerr = getNamespace(ctx, objectStorageClient)
	if cerr != nil {
		return nil, fmt.Errorf("failed to get namespace : %w", cerr)
	}
	return &objectStorageClient, nil
}

func (c *ociObjectStorageClient) pingBucket(logger logger.Logger) error {
	req := objectstorage.GetBucketRequest{
		NamespaceName: &c.objectStorageMetadata.namespace,
		BucketName:    &c.objectStorageMetadata.bucketName,
	}
	_, err := c.objectStorageMetadata.OCIObjectStorageClient.GetBucket(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to retrieve bucket details : %w", err)
	}
	return nil
}
