// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package objectstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/oracle/oci-go-sdk/v54/common"
	"github.com/oracle/oci-go-sdk/v54/objectstorage"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	keyDelimiter   = "||"
	tenancyKey     = "tenancyOCID"
	compartmentKey = "compartmentOCID"
	regionKey      = "region"
	fingerPrintKey = "fingerPrint"
	privateKeyKey  = "privateKey"
	userKey        = "userOCID"
	bucketNameKey  = "bucketName"
)

type StateStore struct {
	state.DefaultBulkStore

	json     jsoniter.API
	features []state.Feature
	logger   logger.Logger
	client   objectStoreClient
}

type Metadata struct {
	userOCID               string
	bucketName             string
	region                 string
	tenancyOCID            string
	fingerPrint            string
	privateKey             string
	compartmentOCID        string
	namespace              string
	OCIObjectStorageClient *objectstorage.ObjectStorageClient
}

type objectStoreClient interface {
	getObject(ctx context.Context, objectname string, logger logger.Logger) ([]byte, *string, error)
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
	var err error
	if meta.bucketName, err = getValue(metadata, bucketNameKey); err != nil {
		return nil, err
	}
	if meta.region, err = getValue(metadata, regionKey); err != nil {
		return nil, err
	}
	if meta.compartmentOCID, err = getValue(metadata, compartmentKey); err != nil {
		return nil, err
	}
	if meta.userOCID, err = getValue(metadata, userKey); err != nil {
		return nil, err
	}
	if meta.fingerPrint, err = getValue(metadata, fingerPrintKey); err != nil {
		return nil, err
	}
	if meta.privateKey, err = getValue(metadata, privateKeyKey); err != nil {
		return nil, err
	}
	if meta.tenancyOCID, err = getValue(metadata, tenancyKey); err != nil {
		return nil, err
	}
	return &meta, nil
}

func getValue(metadata map[string]string, key string) (string, error) {
	if val, ok := metadata[key]; ok && val != "" {
		return val, nil
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

	r.logger.Debugf("Save state in OCI Object Storage Bucket under key %s ", req.Key)
	objectName := getFileName(req.Key)
	content := r.marshal(req)
	objectLength := int64(len(content))
	ctx := context.Background()
	etag := req.ETag
	if req.Options.Concurrency != state.FirstWrite {
		etag = nil
	}
	err := r.client.putObject(ctx, objectName, objectLength, ioutil.NopCloser(bytes.NewReader(content)), nil, etag, r.logger)
	if err != nil {
		r.logger.Debugf("error in writing object to OCI object storage  %s, err %s", req.Key, err)
		return fmt.Errorf("failed to write object to OCI Object storage : %w", err)
	}
	return nil
}

func (r *StateStore) readDocument(req *state.GetRequest) ([]byte, *string, error) {
	if len(req.Key) == 0 || req.Key == "" {
		return nil, nil, fmt.Errorf("key for value to get was missing from request")
	}
	objectName := getFileName(req.Key)
	ctx := context.Background()
	content, etag, err := r.client.getObject(ctx, objectName, r.logger)
	if err != nil {
		r.logger.Debugf("download file %s, err %s", req.Key, err)
		return nil, nil, fmt.Errorf("failed to read object from OCI Object storage : %w", err)
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

	return pr[1]
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

func (c *ociObjectStorageClient) getObject(ctx context.Context, objectname string, logger logger.Logger) ([]byte, *string, error) {
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
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to retrieve object : %w", err)
	}
	if response.ETag != nil {
		logger.Debugf("OCI ObjectStorage StateStore metadata:  ETag %s", *response.ETag)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(response.Content)
	return buf.Bytes(), response.ETag, nil
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
	configurationProvider := common.NewRawConfigurationProvider(c.objectStorageMetadata.tenancyOCID, c.objectStorageMetadata.userOCID, c.objectStorageMetadata.region, c.objectStorageMetadata.fingerPrint, c.objectStorageMetadata.privateKey, nil)
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
