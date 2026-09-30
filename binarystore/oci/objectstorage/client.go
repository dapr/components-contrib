/*
Copyright 2026 The Dapr Authors
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
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v54/common"
	"github.com/oracle/oci-go-sdk/v54/common/auth"
	ociobjectstorage "github.com/oracle/oci-go-sdk/v54/objectstorage"
	"github.com/oracle/oci-go-sdk/v54/objectstorage/transfer"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

// objectStoreClient is the narrow set of object operations the component needs
// from OCI Object Storage; it exists so the store logic can be tested without
// reaching the service.
type objectStoreClient interface {
	putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error
	getObject(ctx context.Context, name string) (io.ReadCloser, error)
	deleteObject(ctx context.Context, name string) error
	close() error
}

type ociObjectStoreClient struct {
	metadata     *objectStoreMetadata
	objectClient *ociobjectstorage.ObjectStorageClient
	httpClient   *http.Client
	logger       logger.Logger
}

// newOCIObjectStoreClient builds an OCI Object Storage client from the parsed
// component metadata, resolving the tenancy namespace and ensuring the target
// bucket exists.
func newOCIObjectStoreClient(ctx context.Context, meta *objectStoreMetadata, log logger.Logger) (*ociObjectStoreClient, error) {
	provider, err := newConfigurationProvider(meta)
	if err != nil {
		return nil, err
	}

	objectClient, httpClient, err := newObjectClient(provider)
	if err != nil {
		return nil, err
	}

	c := &ociObjectStoreClient{
		metadata:     meta,
		objectClient: objectClient,
		httpClient:   httpClient,
		logger:       log,
	}

	if c.metadata.Namespace == "" {
		if c.metadata.Namespace, err = c.getNamespace(ctx); err != nil {
			c.close() //nolint:errcheck
			return nil, err
		}
	}
	if err = c.ensureBucketExists(ctx); err != nil {
		c.close() //nolint:errcheck
		return nil, err
	}
	return c, nil
}

// newConfigurationProvider selects the OCI authentication mechanism requested
// by the component metadata.
func newConfigurationProvider(meta *objectStoreMetadata) (common.ConfigurationProvider, error) {
	switch {
	case meta.InstancePrincipalAuthentication:
		provider, err := auth.InstancePrincipalConfigurationProvider()
		if err != nil {
			return nil, fmt.Errorf("failed to get OCI instance principal configuration provider: %w", err)
		}
		return provider, nil
	case meta.ConfigFileAuthentication:
		return common.CustomProfileConfigProvider(meta.ConfigFilePath, meta.ConfigFileProfile), nil
	default:
		return common.NewRawConfigurationProvider(
			meta.TenancyOCID,
			meta.UserOCID,
			meta.Region,
			meta.FingerPrint,
			meta.PrivateKey,
			nil,
		), nil
	}
}

// newObjectClient creates the SDK client along with the *http.Client backing
// it. The SDK does not expose its dispatcher as a closable type, so the
// transport is supplied here and returned to the caller, allowing the
// component to release the connection pool on Close.
func newObjectClient(provider common.ConfigurationProvider) (*ociobjectstorage.ObjectStorageClient, *http.Client, error) {
	client, err := ociobjectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ObjectStorageClient: %w", err)
	}

	// No client-level timeout is set: uploads and downloads stream payloads of
	// unbounded size, so per-request deadlines come from the caller's context
	// instead.
	httpClient := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	client.HTTPClient = httpClient
	return &client, httpClient, nil
}

func (c *ociObjectStoreClient) getNamespace(ctx context.Context) (string, error) {
	resp, err := c.objectClient.GetNamespace(ctx, ociobjectstorage.GetNamespaceRequest{})
	if err != nil {
		return "", fmt.Errorf("failed to retrieve tenancy namespace: %w", err)
	}
	if resp.Value == nil || *resp.Value == "" {
		return "", errors.New("failed to retrieve tenancy namespace: empty namespace")
	}
	return *resp.Value, nil
}

func (c *ociObjectStoreClient) ensureBucketExists(ctx context.Context) error {
	_, err := c.objectClient.GetBucket(ctx, ociobjectstorage.GetBucketRequest{
		NamespaceName: &c.metadata.Namespace,
		BucketName:    &c.metadata.BucketName,
	})
	if err == nil {
		return nil
	}
	if !isNotFound(err) {
		return fmt.Errorf("failed to retrieve bucket details: %w", err)
	}

	_, err = c.objectClient.CreateBucket(ctx, ociobjectstorage.CreateBucketRequest{
		NamespaceName: &c.metadata.Namespace,
		CreateBucketDetails: ociobjectstorage.CreateBucketDetails{
			CompartmentId:    &c.metadata.CompartmentOCID,
			Name:             &c.metadata.BucketName,
			Metadata:         map[string]string{},
			PublicAccessType: ociobjectstorage.CreateBucketDetailsPublicAccessTypeNopublicaccess,
		},
	})
	if err != nil {
		if isAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	c.logger.Debugf("Created OCI Object Storage bucket %s for BinaryStore", c.metadata.BucketName)
	return nil
}

func (c *ociObjectStoreClient) putObject(ctx context.Context, name string, data io.Reader, overwrite bool) error {
	// transfer.UploadManager.UploadStream only short-circuits a zero-length
	// body to a single PutObject when the reader is one of a fixed set of
	// concrete types (*bytes.Buffer, *bytes.Reader, *strings.Reader,
	// *os.File); anything else - including the streaming reader supplied by
	// the runtime for req.Data - falls through to the multipart path, which
	// emits zero parts for an empty stream and fails when the commit is
	// attempted. Peek the first byte ourselves so an empty payload is always
	// represented as a *bytes.Reader that the SDK recognises as zero-length.
	peek := make([]byte, 1)
	n, err := io.ReadFull(data, peek)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("failed to read object data: %w", err)
	}

	var streamReader io.Reader
	if n == 0 {
		streamReader = bytes.NewReader(nil)
	} else {
		streamReader = io.MultiReader(bytes.NewReader(peek[:n]), data)
	}

	req := transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:       &c.metadata.Namespace,
			BucketName:          &c.metadata.BucketName,
			ObjectName:          &name,
			ObjectStorageClient: c.objectClient,
			// Set explicitly so buffering matches the other binary store
			// providers rather than the SDK defaults (10 MiB parts, 5
			// goroutines). The shared part size is deliberately at or above
			// OCI's 10 MiB minimum for non-final multipart parts.
			PartSize:           common.Int64(binarystore.DefaultUploadPartSize),
			NumberOfGoroutines: common.Int(binarystore.DefaultUploadConcurrency),
		},
		StreamReader: streamReader,
	}
	if !overwrite {
		req.IfNoneMatch = common.String("*")
	}

	resp, err := transfer.NewUploadManager().UploadStream(ctx, req)
	if err != nil && resp.MultipartUploadResponse != nil && resp.UploadID != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		_, abortErr := c.objectClient.AbortMultipartUpload(cleanupCtx, ociobjectstorage.AbortMultipartUploadRequest{
			NamespaceName: &c.metadata.Namespace,
			BucketName:    &c.metadata.BucketName,
			ObjectName:    &name,
			UploadId:      resp.UploadID,
		})
		if abortErr != nil {
			c.logger.Warnf("failed to abort OCI multipart upload for object %q: %v", name, abortErr)
		}
	}
	return err
}

func (c *ociObjectStoreClient) getObject(ctx context.Context, name string) (io.ReadCloser, error) {
	resp, err := c.objectClient.GetObject(ctx, ociobjectstorage.GetObjectRequest{
		NamespaceName: &c.metadata.Namespace,
		BucketName:    &c.metadata.BucketName,
		ObjectName:    &name,
	})
	if err != nil {
		return nil, err
	}
	return resp.Content, nil
}

func (c *ociObjectStoreClient) deleteObject(ctx context.Context, name string) error {
	// OCI Object Storage returns 404 ObjectNotFound from DeleteObject for a
	// missing key, so no existence pre-check is required; the caller maps the
	// error through isNotFound to produce binarystore.ErrFileNotFound.
	_, err := c.objectClient.DeleteObject(ctx, ociobjectstorage.DeleteObjectRequest{
		NamespaceName: &c.metadata.Namespace,
		BucketName:    &c.metadata.BucketName,
		ObjectName:    &name,
	})
	return err
}

// close releases the connections held by the transport backing the SDK client.
// The OCI SDK has no Close method of its own, so draining the pool owned by
// newObjectClient is what shuts the connections down.
func (c *ociObjectStoreClient) close() error {
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return nil
}

type serviceError interface {
	GetHTTPStatusCode() int
	GetCode() string
}

func isNotFound(err error) bool {
	var se serviceError
	if errors.As(err, &se) {
		return se.GetHTTPStatusCode() == http.StatusNotFound
	}
	return false
}

func isPreconditionFailed(err error) bool {
	var se serviceError
	if errors.As(err, &se) {
		return se.GetHTTPStatusCode() == http.StatusPreconditionFailed ||
			strings.EqualFold(se.GetCode(), "PreconditionFailed") ||
			strings.EqualFold(se.GetCode(), "ConditionNotMet") ||
			(se.GetHTTPStatusCode() == http.StatusConflict &&
				strings.Contains(strings.ToLower(se.GetCode()), "alreadyexists"))
	}
	return false
}

func isAlreadyExists(err error) bool {
	var se serviceError
	if errors.As(err, &se) {
		code := strings.ToLower(se.GetCode())
		return se.GetHTTPStatusCode() == http.StatusConflict &&
			(strings.Contains(code, "alreadyexists") || strings.Contains(code, "bucketalreadyexists"))
	}
	return false
}
