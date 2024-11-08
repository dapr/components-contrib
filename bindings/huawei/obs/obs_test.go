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

package obs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// MockHuaweiOBSService is a mock service layer which mimics the OBS API functions
// and it implements the HuaweiOBSAPI through stubs.
type MockHuaweiOBSService struct {
	PutObjectFn    func(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error)
	PutFileFn      func(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error)
	GetObjectFn    func(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error)
	DeleteObjectFn func(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error)
	ListObjectsFn  func(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error)
}

func (m *MockHuaweiOBSService) PutObject(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
	return m.PutObjectFn(ctx, input)
}

func (m *MockHuaweiOBSService) PutFile(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
	return m.PutFileFn(ctx, input)
}

func (m *MockHuaweiOBSService) GetObject(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
	return m.GetObjectFn(ctx, input)
}

func (m *MockHuaweiOBSService) DeleteObject(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
	return m.DeleteObjectFn(ctx, input)
}

func (m *MockHuaweiOBSService) ListObjects(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
	return m.ListObjectsFn(ctx, input)
}

func (m *MockHuaweiOBSService) Close() {}

func TestParseMetadata(t *testing.T) {
	obs := NewHuaweiOBS(logger.NewLogger("test")).(*HuaweiOBS)

	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}

		meta, err := obs.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "dummy-bucket", meta.Bucket)
		assert.Equal(t, "dummy-endpoint", meta.Endpoint)
		assert.Equal(t, "dummy-ak", meta.AccessKey)
		assert.Equal(t, "dummy-sk", meta.SecretKey)
	})
}

func TestInit(t *testing.T) {
	obs := NewHuaweiOBS(logger.NewLogger("test"))

	t.Run("Successful init with correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(context.Background(), m)
		require.NoError(t, err)
	})
	t.Run("Init with missing bucket name", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(context.Background(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing obs bucket name"))
	})
	t.Run("Init with missing access key", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(context.Background(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing the huawei access key"))
	})
	t.Run("Init with missing secret key", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
		}
		err := obs.Init(context.Background(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing the huawei secret key"))
	})
	t.Run("Init with missing endpoint", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(context.Background(), m)
		require.Error(t, err)
		assert.Equal(t, err, errors.New("missing obs endpoint"))
	})
}

func TestOperations(t *testing.T) {
	obs := NewHuaweiOBS(logger.NewLogger("test"))

	t.Run("Count supported operations", func(t *testing.T) {
		ops := obs.Operations()
		assert.Len(t, ops, 5)
	})
}

func TestCreateOperation(t *testing.T) {
	t.Run("Successfully create object with key", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
					return &obs.PutObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "create",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte(`"Hello OBS"`),
		}

		out, err := mo.create(context.Background(), req)
		require.NoError(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Successfully create object with uuid", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
					return &obs.PutObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "create",
			Data:      []byte(`"Hello OBS"`),
		}

		out, err := mo.create(context.Background(), req)
		require.NoError(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Successfully create null object with no data", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
					return &obs.PutObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "create",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.create(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Fail create object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
					return nil, errors.New("error while creating object")
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "create",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte(`"Hello OBS"`),
		}

		_, err := mo.create(context.Background(), req)
		require.Error(t, err)
	})
}

func TestUploadOperation(t *testing.T) {
	t.Run("Successfully upload object with key", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutFileFn: func(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
					return &obs.PutObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "upload",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte(`{"sourceFile": "dummy-path"}`),
		}

		out, err := mo.upload(context.Background(), req)
		require.NoError(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Successfully upload object with uuid", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutFileFn: func(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
					return &obs.PutObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "upload",
			Data:      []byte(`{"sourceFile": "dummy-path"}`),
		}

		out, err := mo.upload(context.Background(), req)
		require.NoError(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Fail upload object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutFileFn: func(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
					return nil, errors.New("error while creating object")
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "upload",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte(`{"sourceFile": "dummy-path"}`),
		}

		_, err := mo.upload(context.Background(), req)
		require.Error(t, err)
	})
}

func TestGetOperation(t *testing.T) {
	t.Run("Successfully get object", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return &obs.GetObjectOutput{
						GetObjectMetadataOutput: obs.GetObjectMetadataOutput{
							BaseModel: obs.BaseModel{
								StatusCode: 200,
							},
							Metadata: map[string]string{},
						},
						Body: io.NopCloser(strings.NewReader("Hello Dapr")),
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "get",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.get(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Fail get object with no key metadata", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{},
			logger:  logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "get",
		}

		_, err := mo.get(context.Background(), req)
		require.Error(t, err)
	})

	t.Run("Fail get object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return nil, errors.New("error while getting object")
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "get",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.get(context.Background(), req)
		require.Error(t, err)
	})

	t.Run("Fail get object with no response data", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return &obs.GetObjectOutput{
						GetObjectMetadataOutput: obs.GetObjectMetadataOutput{
							BaseModel: obs.BaseModel{
								StatusCode: 200,
							},
							Metadata: map[string]string{},
						},
						Body: io.NopCloser(iotest.ErrReader(errors.New("unexpected data reading error"))),
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "get",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.get(context.Background(), req)
		require.Error(t, err)
	})
}

func TestDeleteOperation(t *testing.T) {
	t.Run("Successfully delete object", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				DeleteObjectFn: func(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
					return &obs.DeleteObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "delete",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		out, err := mo.delete(context.Background(), req)
		require.NoError(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Fail delete object with no key metadata", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{},
			logger:  logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "delete",
		}

		_, err := mo.delete(context.Background(), req)
		require.Error(t, err)
	})

	t.Run("Fail delete object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				DeleteObjectFn: func(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
					return nil, errors.New("error while deleting object")
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "delete",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.delete(context.Background(), req)
		require.Error(t, err)
	})
}

func TestListOperation(t *testing.T) {
	t.Run("Successfully list objects", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
					return &obs.ListObjectsOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "list",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte("{\"maxResults\": 10}"),
		}

		_, err := mo.list(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Fail list objects with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
					return nil, errors.New("error while listing objects")
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "list",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte("{\"maxResults\": 10}"),
		}

		_, err := mo.list(context.Background(), req)
		require.Error(t, err)
	})

	t.Run("Successfully list objects with default maxResults", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
					return &obs.ListObjectsOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "list",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte("{\"key\": \"value\"}"),
		}

		_, err := mo.list(context.Background(), req)
		require.NoError(t, err)
	})
}

func TestInvoke(t *testing.T) {
	t.Run("Successfully invoke create", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
					return &obs.PutObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "create",
		}

		_, err := mo.Invoke(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully invoke get", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return &obs.GetObjectOutput{
						GetObjectMetadataOutput: obs.GetObjectMetadataOutput{
							BaseModel: obs.BaseModel{
								StatusCode: 200,
							},
							Metadata: map[string]string{},
						},
						Body: io.NopCloser(strings.NewReader("Hello Dapr")),
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "get",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.Invoke(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully invoke delete", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				DeleteObjectFn: func(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
					return &obs.DeleteObjectOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 204,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "delete",
			Metadata: map[string]string{
				metadataKey: "test",
			},
		}

		_, err := mo.Invoke(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Successfully invoke list", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
					return &obs.ListObjectsOutput{
						BaseModel: obs.BaseModel{
							StatusCode: 200,
						},
					}, nil
				},
			},
			logger: logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "list",
			Metadata: map[string]string{
				metadataKey: "test",
			},
			Data: []byte("{\"maxResults\": 10}"),
		}

		_, err := mo.Invoke(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("Error invoke unknown operation type", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{},
			logger:  logger.NewLogger("test"),
			metadata: &obsMetadata{
				Bucket: "test",
			},
		}

		req := &bindings.InvokeRequest{
			Operation: "unknown",
		}

		_, err := mo.Invoke(context.Background(), req)
		require.Error(t, err)
	})
}
