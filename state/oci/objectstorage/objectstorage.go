/*
OCI Object Storage state store.

Sample configuration in yaml:

	apiVersion: dapr.io/v1alpha1
	kind: Component
	metadata:
	  name: statestore
	spec:
	  type: state.oci.objectstorage
	  metadata:
	  - name: tenancyOCID
		value: <tenancyOCID>
	  - name: userOCID
		value: <userOCID>
	  - name: fingerPrint
		value: <fingerPrint>
	  - name: region
		value: <region>
	  - name: bucketName
		value: <bucket Name>
      - name: compartmentOCID
        value: ocid1.compartment.oc1..aaaaaaaacsssekayq4d34nl5h3eqs5e6ak3j5s4jhlws6oxf7rr5pxmt3zrq
      - name: privateKey
        value: |
               -----BEGIN RSA PRIVATE KEY-----
               XAS
               -----END RSA PRIVATE KEY-----

*/

package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/oracle/oci-go-sdk/v54/common"

	"github.com/oracle/oci-go-sdk/v54/objectstorage"
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

	json                  jsoniter.API
	features              []state.Feature
	logger                logger.Logger
	objectStorageMetadata *Metadata
}

type Metadata struct {
	userOCID            string
	bucketName          string
	region              string
	tenancyOCID         string
	fingerPrint         string
	privateKey          string
	compartmentOCID     string
	namespace           string
	objectStorageClient *objectstorage.ObjectStorageClient
}

/*********  Interface Implementations Init, Features, Get, Set, Delete and the instantiation function NewOCIObjectStorageStore. */

func (r *StateStore) Init(metadata state.Metadata) error {
	r.logger.Debugf("Init OCI Object Storage State Store")
	meta, err := getObjectStorageMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	err = initStorageClientAndBucket(meta, r.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize client or create bucket : %w", err)
	}
	r.logger.Debugf("OCI Object Storage State Store initialized using bucket '%s'", meta.bucketName)
	r.objectStorageMetadata = meta
	return nil
}

func initStorageClientAndBucket(meta *Metadata, logger logger.Logger) error {
	logger.Debugf("1")
	configurationProvider := common.NewRawConfigurationProvider(meta.tenancyOCID, meta.userOCID, meta.region, meta.fingerPrint, meta.privateKey, nil)
	logger.Debugf("2")
	objectStorageClient, cerr := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	logger.Debugf("3")
	if cerr != nil {
		return fmt.Errorf("failed to create ObjectStorageClient : %w", cerr)
	}
	meta.objectStorageClient = &objectStorageClient
	ctx := context.Background()
	meta.namespace, cerr = getNamespace(ctx, objectStorageClient)
	logger.Debugf("4 %w", cerr)
	if cerr != nil {
		return fmt.Errorf("failed to get namespace : %w", cerr)
	}
	cerr = ensureBucketExists(ctx, objectStorageClient, meta.namespace, meta.bucketName, meta.compartmentOCID, logger)
	if cerr != nil {
		return fmt.Errorf("failed to read or create bucket : %w", cerr)
	}
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
		json:                  jsoniter.ConfigFastest,
		features:              []state.Feature{state.FeatureETag},
		logger:                logger,
		objectStorageMetadata: nil,
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

func (r *StateStore) writeDocument(req *state.SetRequest) error {
	if len(req.Key) == 0 || req.Key == "" {
		return fmt.Errorf("key for value to set was missing from request")
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

	err := putObject(ctx, *r.objectStorageMetadata.objectStorageClient, r.objectStorageMetadata.namespace, r.objectStorageMetadata.bucketName, objectName, objectLength, ioutil.NopCloser(bytes.NewReader(content)), nil, etag, r.logger)
	return err
}

func (r *StateStore) readDocument(req *state.GetRequest) ([]byte, *string, error) {
	objectName := getFileName(req.Key)
	ctx := context.Background()
	content, etag, err := getObject(ctx, *r.objectStorageMetadata.objectStorageClient, r.objectStorageMetadata.namespace, r.objectStorageMetadata.bucketName, objectName, r.logger)
	if err != nil {
		r.logger.Debugf("download file %s, err %s", req.Key, err)
		return nil, nil, err
	}
	return content, etag, err
}

func (r *StateStore) pingBucket() error {
	req := objectstorage.GetBucketRequest{
		NamespaceName: &r.objectStorageMetadata.namespace,
		BucketName:    &r.objectStorageMetadata.bucketName,
	}
	// Send the request using the service client.
	_, err := r.objectStorageMetadata.objectStorageClient.GetBucket(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to retrieve bucket details : %w", err)
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
	err := deleteObject(ctx, *r.objectStorageMetadata.objectStorageClient, r.objectStorageMetadata.namespace, r.objectStorageMetadata.bucketName, objectName, etag)
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

func deleteObject(ctx context.Context, client objectstorage.ObjectStorageClient, namespace, bucketname, objectname string, etag *string) (err error) {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
		IfMatch:       etag,
	}
	_, err = client.DeleteObject(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to delete object from OCI : %w", err)
	}
	return nil
}

func getObject(ctx context.Context, client objectstorage.ObjectStorageClient, namespace string, bucketname string, objectname string, logger logger.Logger) ([]byte, *string, error) {
	logger.Debugf("read file %s from OCI ObjectStorage StateStore %s ", objectname, bucketname)
	request := objectstorage.GetObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
	}
	response, err := client.GetObject(ctx, request)
	if err != nil {
		logger.Debugf("Issue in OCI ObjectStorage with retrieving object %s, error:  %s", objectname, err)
		if response.RawResponse.StatusCode == 404 {
			return nil, nil, errors.New("ObjectNotFound")
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

func putObject(ctx context.Context, client objectstorage.ObjectStorageClient, namespace, bucketname, objectname string, contentLen int64, content io.ReadCloser, metadata map[string]string, etag *string, logger logger.Logger) error {
	request := objectstorage.PutObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
		ContentLength: &contentLen,
		PutObjectBody: content,
		OpcMeta:       metadata,
		IfMatch:       etag,
	}
	_, err := client.PutObject(ctx, request)
	logger.Debugf("Put object ", objectname, " in bucket ", bucketname)
	if err != nil {
		return fmt.Errorf("failed to put object on OCI : %w", err)
	}
	return nil
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
