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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/oracle/oci-go-sdk/v54/common"
	"github.com/oracle/oci-go-sdk/v54/common/auth"
	"github.com/oracle/oci-go-sdk/v54/objectstorage"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	keyDelimiter                       = "||"
	instancePrincipalAuthenticationKey = "instancePrincipalAuthentication"
	configFileAuthenticationKey        = "configFileAuthentication"
	configFilePathKey                  = "configFilePath"
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
	state.BulkStore

	json     jsoniter.API
	features []state.Feature
	logger   logger.Logger
	client   objectStoreClient
}

type objectStoreMetadata struct {
	UserOCID                        string
	BucketName                      string
	Region                          string
	TenancyOCID                     string
	FingerPrint                     string
	PrivateKey                      string
	CompartmentOCID                 string
	Namespace                       string
	ConfigFilePath                  string
	ConfigFileProfile               string
	InstancePrincipalAuthentication bool
	ConfigFileAuthentication        bool

	OCIObjectStorageClient *objectstorage.ObjectStorageClient
}

type objectStoreClient interface {
	getObject(ctx context.Context, objectname string) (content []byte, etag *string, metadata map[string]string, err error)
	deleteObject(ctx context.Context, objectname string, etag *string) (err error)
	putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string) error
	initStorageBucket(ctx context.Context) error
	initOCIObjectStorageClient(ctx context.Context) (*objectstorage.ObjectStorageClient, error)
	pingBucket(ctx context.Context) error
}

type objectStorageClient struct {
	objectStorageMetadata *objectStoreMetadata
}

type ociObjectStorageClient struct {
	objectStorageClient
	logger logger.Logger
}

/*********  Interface Implementations Init, Features, Get, Set, Delete and the instantiation function NewOCIObjectStorageStore. */

func (r *StateStore) Init(ctx context.Context, metadata state.Metadata) error {
	r.logger.Debugf("Init OCI Object Storage State Store")
	meta, err := getObjectStorageMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	r.client = &ociObjectStorageClient{
		objectStorageClient: objectStorageClient{
			objectStorageMetadata: meta,
		},
		logger: r.logger,
	}

	objectStorageClient, cerr := r.client.initOCIObjectStorageClient(ctx)
	if cerr != nil {
		return fmt.Errorf("failed to initialize client or create bucket : %w", cerr)
	}
	meta.OCIObjectStorageClient = objectStorageClient

	cerr = r.client.initStorageBucket(ctx)
	if cerr != nil {
		return fmt.Errorf("failed to create bucket : %w", cerr)
	}
	r.logger.Debugf("OCI Object Storage State Store initialized using bucket '%s'", meta.BucketName)

	return nil
}

func (r *StateStore) Features() []state.Feature {
	return r.features
}

func (r *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	r.logger.Debugf("Delete entry from OCI Object Storage State Store with key ", req.Key)
	err := r.deleteDocument(ctx, req)
	return err
}

func (r *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	r.logger.Debugf("Get from OCI Object Storage State Store with key ", req.Key)
	content, etag, err := r.readDocument(ctx, req)
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

func (r *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	r.logger.Debugf("saving %s to OCI Object Storage State Store", req.Key)
	return r.writeDocument(ctx, req)
}

func (r *StateStore) Ping(ctx context.Context) error {
	return r.pingBucket(ctx)
}

func (r *StateStore) Close() error {
	return nil
}

func NewOCIObjectStorageStore(logger logger.Logger) state.Store {
	s := &StateStore{
		json: jsoniter.ConfigFastest,
		features: []state.Feature{
			state.FeatureETag,
			state.FeatureTTL,
		},
		logger: logger,
		client: nil,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)

	return s
}

/************** private helper functions. */

func getConfigFilePath(configFilePath string) (value string, err error) {
	value = configFilePath
	if strings.HasPrefix(value, "~/") {
		return "", fmt.Errorf("%s is set to %s which starts with ~/; this is not supported - please provide absolute path to configuration file", configFilePathKey, value)
	}
	if value != "" {
		if _, err = os.Stat(value); err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("oci configuration file %s does not exist %w", value, err)
			}
			return "", fmt.Errorf("error %w with reading oci configuration file %s", err, value)
		}
	}
	return value, nil
}

func getObjectStorageMetadata(meta map[string]string) (*objectStoreMetadata, error) {
	m := objectStoreMetadata{}
	errDecode := kitmd.DecodeMetadata(meta, &m)
	if errDecode != nil {
		return nil, errDecode
	}

	var err error

	if m.ConfigFileAuthentication {
		if m.ConfigFilePath, err = getConfigFilePath(m.ConfigFilePath); err != nil {
			return nil, err
		}
	}
	if m.BucketName == "" {
		return nil, fmt.Errorf("missing or empty %s field from metadata", bucketNameKey)
	}
	if m.CompartmentOCID == "" {
		return nil, fmt.Errorf("missing or empty %s field from metadata", compartmentKey)
	}
	externalAuthentication := m.InstancePrincipalAuthentication || m.ConfigFileAuthentication
	if !externalAuthentication {
		err = getIdentityAuthenticationDetails(m)
		if err != nil {
			return nil, err
		}
	}
	return &m, nil
}

func getIdentityAuthenticationDetails(meta objectStoreMetadata) (err error) {
	if meta.Region == "" {
		return fmt.Errorf("missing or empty %s field from metadata", regionKey)
	}
	if meta.UserOCID == "" {
		return fmt.Errorf("missing or empty %s field from metadata", userKey)
	}
	if meta.FingerPrint == "" {
		return fmt.Errorf("missing or empty %s field from metadata", fingerPrintKey)
	}
	if meta.PrivateKey == "" {
		return fmt.Errorf("missing or empty %s field from metadata", privateKeyKey)
	}
	if meta.TenancyOCID == "" {
		return fmt.Errorf("missing or empty %s field from metadata", tenancyKey)
	}
	return nil
}

// functions that bridge from the Dapr State API to the OCI ObjectStorage Client.
func (r *StateStore) writeDocument(ctx context.Context, req *state.SetRequest) error {
	if len(req.Key) == 0 || req.Key == "" {
		return errors.New("key for value to set was missing from request")
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || len(*req.ETag) == 0) {
		r.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return errors.New("when FirstWrite is to be enforced, a value must be provided for the ETag")
	}
	metadata := (map[string]string{"category": daprStateStoreMetaLabel})

	err := r.convertTTLtoExpiryTime(req, metadata)
	if err != nil {
		return fmt.Errorf("failed to process ttl meta data: %w", err)
	}

	r.logger.Debugf("Save state in OCI Object Storage Bucket under key %s ", req.Key)
	objectName := getFileName(req.Key)
	content := r.marshal(req)
	objectLength := int64(len(content))
	etag := req.ETag
	if req.Options.Concurrency != state.FirstWrite {
		etag = nil
	}
	err = r.client.putObject(ctx, objectName, objectLength, io.NopCloser(bytes.NewReader(content)), metadata, etag)
	if err != nil {
		r.logger.Debugf("error in writing object to OCI object storage  %s, err %s", req.Key, err)
		return fmt.Errorf("failed to write object to OCI Object storage : %w", err)
	}
	return nil
}

func (r *StateStore) convertTTLtoExpiryTime(req *state.SetRequest, metadata map[string]string) error {
	ttl, ttlerr := stateutils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}
	if ttl != nil {
		metadata[expiryTimeMetaLabel] = time.Now().UTC().Add(time.Second * time.Duration(*ttl)).Format(isoDateTimeFormat)
		r.logger.Debugf("Set %s in meta properties for object to ", expiryTimeMetaLabel, metadata[expiryTimeMetaLabel])
	}
	return nil
}

func (r *StateStore) readDocument(ctx context.Context, req *state.GetRequest) ([]byte, *string, error) {
	if len(req.Key) == 0 || req.Key == "" {
		return nil, nil, errors.New("key for value to get was missing from request")
	}
	objectName := getFileName(req.Key)
	content, etag, meta, err := r.client.getObject(ctx, objectName)
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

func (r *StateStore) pingBucket(ctx context.Context) error {
	err := r.client.pingBucket(ctx)
	if err != nil {
		r.logger.Debugf("ping bucket failed err %s", err)
		return fmt.Errorf("failed to ping bucket on OCI Object storage : %w", err)
	}
	return nil
}

func (r *StateStore) deleteDocument(ctx context.Context, req *state.DeleteRequest) error {
	if len(req.Key) == 0 || req.Key == "" {
		return errors.New("key for value to delete was missing from request")
	}

	objectName := getFileName(req.Key)
	etag := req.ETag
	if req.Options.Concurrency != state.FirstWrite {
		etag = nil
	}
	if req.Options.Concurrency == state.FirstWrite && (etag == nil || len(*etag) == 0) {
		r.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return errors.New("when FirstWrite is to be enforced, a value must be provided for the ETag")
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
func (c *ociObjectStorageClient) ensureBucketExists(ctx context.Context, client objectstorage.ObjectStorageClient, namespace string, name string, compartmentOCID string) error {
	req := objectstorage.GetBucketRequest{
		NamespaceName: &namespace,
		BucketName:    &name,
	}
	// verify if bucket exists.
	response, err := client.GetBucket(ctx, req)
	if err != nil {
		if response.RawResponse.StatusCode == http.StatusNotFound {
			err = createBucket(ctx, client, namespace, name, compartmentOCID)
			if err == nil {
				c.logger.Debugf("Created OCI Object Storage Bucket %s as State Store", name)
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

func (c *ociObjectStorageClient) getObject(ctx context.Context, objectname string) (content []byte, etag *string, metadata map[string]string, err error) {
	c.logger.Debugf("read file %s from OCI ObjectStorage StateStore %s ", objectname, &c.objectStorageMetadata.BucketName)
	request := objectstorage.GetObjectRequest{
		NamespaceName: &c.objectStorageMetadata.Namespace,
		BucketName:    &c.objectStorageMetadata.BucketName,
		ObjectName:    &objectname,
	}
	response, err := c.objectStorageMetadata.OCIObjectStorageClient.GetObject(ctx, request)
	if err != nil {
		c.logger.Debugf("Issue in OCI ObjectStorage with retrieving object %s, error:  %s", objectname, err)
		if response.RawResponse.StatusCode == http.StatusNotFound {
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
		NamespaceName: &c.objectStorageMetadata.Namespace,
		BucketName:    &c.objectStorageMetadata.BucketName,
		ObjectName:    &objectname,
		IfMatch:       etag,
	}
	_, err = c.objectStorageMetadata.OCIObjectStorageClient.DeleteObject(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to delete object from OCI : %w", err)
	}
	return nil
}

func (c *ociObjectStorageClient) putObject(ctx context.Context, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string) error {
	request := objectstorage.PutObjectRequest{
		NamespaceName: &c.objectStorageMetadata.Namespace,
		BucketName:    &c.objectStorageMetadata.BucketName,
		ObjectName:    &objectname,
		ContentLength: &contentLen,
		PutObjectBody: content,
		OpcMeta:       metadata,
		IfMatch:       etag,
	}
	_, err := c.objectStorageMetadata.OCIObjectStorageClient.PutObject(ctx, request)
	c.logger.Debugf("Put object ", objectname, " in bucket ", &c.objectStorageMetadata.BucketName)
	if err != nil {
		return fmt.Errorf("failed to put object on OCI : %w", err)
	}
	return nil
}

func (c *ociObjectStorageClient) initStorageBucket(ctx context.Context) error {
	err := c.ensureBucketExists(ctx, *c.objectStorageMetadata.OCIObjectStorageClient, c.objectStorageMetadata.Namespace, c.objectStorageMetadata.BucketName, c.objectStorageMetadata.CompartmentOCID)
	if err != nil {
		return fmt.Errorf("failed to read or create bucket : %w", err)
	}
	return nil
}

func (c *ociObjectStorageClient) initOCIObjectStorageClient(ctx context.Context) (*objectstorage.ObjectStorageClient, error) {
	var configurationProvider common.ConfigurationProvider
	if c.objectStorageMetadata.InstancePrincipalAuthentication {
		c.logger.Debugf("instance principal authentication is used. ")
		var err error
		configurationProvider, err = auth.InstancePrincipalConfigurationProvider()
		if err != nil {
			return nil, fmt.Errorf("failed to get oci configurationprovider based on instance principal authentication : %w", err)
		}
	} else {
		if c.objectStorageMetadata.ConfigFileAuthentication {
			c.logger.Debugf("configuration file based authentication is used with configuration file path %s and configuration profile %s. ", c.objectStorageMetadata.ConfigFilePath, c.objectStorageMetadata.ConfigFileProfile)
			configurationProvider = common.CustomProfileConfigProvider(c.objectStorageMetadata.ConfigFilePath, c.objectStorageMetadata.ConfigFileProfile)
		} else {
			c.logger.Debugf("identity authentication is used with configuration provided through Dapr component configuration ")
			configurationProvider = common.NewRawConfigurationProvider(c.objectStorageMetadata.TenancyOCID, c.objectStorageMetadata.UserOCID, c.objectStorageMetadata.Region, c.objectStorageMetadata.FingerPrint, c.objectStorageMetadata.PrivateKey, nil)
		}
	}

	objectStorageClient, cerr := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	if cerr != nil {
		return nil, fmt.Errorf("failed to create ObjectStorageClient : %w", cerr)
	}
	c.objectStorageMetadata.Namespace, cerr = getNamespace(ctx, objectStorageClient)
	if cerr != nil {
		return nil, fmt.Errorf("failed to get namespace : %w", cerr)
	}
	return &objectStorageClient, nil
}

func (c *ociObjectStorageClient) pingBucket(ctx context.Context) error {
	req := objectstorage.GetBucketRequest{
		NamespaceName: &c.objectStorageMetadata.Namespace,
		BucketName:    &c.objectStorageMetadata.BucketName,
	}
	_, err := c.objectStorageMetadata.OCIObjectStorageClient.GetBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to retrieve bucket details : %w", err)
	}
	return nil
}

func (r *StateStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := objectStoreMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
