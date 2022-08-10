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
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// MockHuaweiOBSService is a mock service layer which mimics the OBS API functions
// and it implements the HuaweiOBSAPI through stubs.
type MockHuaweiOBSService struct {
	PutObjectFn    func(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error)
	PutFileFn      func(input *obs.PutFileInput) (output *obs.PutObjectOutput, err error)
	GetObjectFn    func(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error)
	DeleteObjectFn func(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error)
	ListObjectsFn  func(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error)
}

func (m *MockHuaweiOBSService) PutObject(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
	return m.PutObjectFn(input)
}

func (m *MockHuaweiOBSService) PutFile(input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
	return m.PutFileFn(input)
}

func (m *MockHuaweiOBSService) GetObject(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
	return m.GetObjectFn(input)
}

func (m *MockHuaweiOBSService) DeleteObject(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
	return m.DeleteObjectFn(input)
}

func (m *MockHuaweiOBSService) ListObjects(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
	return m.ListObjectsFn(input)
}

func TestParseMetadata(t *testing.T) {
	obs := NewHuaweiOBS(logger.NewLogger("test"))

	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}

		meta, err := obs.parseMetadata(m)
		assert.Nil(t, err)
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
		err := obs.Init(m)
		assert.Nil(t, err)
	})
	t.Run("Init with missing bucket name", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing obs bucket name"))
	})
	t.Run("Init with missing access key", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing the huawei access key"))
	})
	t.Run("Init with missing secret key", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"endpoint":  "dummy-endpoint",
			"accessKey": "dummy-ak",
		}
		err := obs.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing the huawei secret key"))
	})
	t.Run("Init with missing endpoint", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"bucket":    "dummy-bucket",
			"accessKey": "dummy-ak",
			"secretKey": "dummy-sk",
		}
		err := obs.Init(m)
		assert.NotNil(t, err)
		assert.Equal(t, err, fmt.Errorf("missing obs endpoint"))
	})
}

func TestOperations(t *testing.T) {
	obs := NewHuaweiOBS(logger.NewLogger("test"))

	t.Run("Count supported operations", func(t *testing.T) {
		ops := obs.Operations()
		assert.Equal(t, 5, len(ops))
	})
}

func TestCreateOperation(t *testing.T) {
	t.Run("Successfully create object with key", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
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

		out, err := mo.create(req)
		assert.Nil(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		assert.Nil(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Successfully create object with uuid", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
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

		out, err := mo.create(req)
		assert.Nil(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		assert.Nil(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Successfully create null object with no data", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
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

		_, err := mo.create(req)
		assert.Nil(t, err)
	})

	t.Run("Fail create object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
					return nil, fmt.Errorf("error while creating object")
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

		_, err := mo.create(req)
		assert.NotNil(t, err)
	})
}

func TestUploadOperation(t *testing.T) {
	t.Run("Successfully upload object with key", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutFileFn: func(input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
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

		out, err := mo.upload(req)
		assert.Nil(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		assert.Nil(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Successfully upload object with uuid", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutFileFn: func(input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
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

		out, err := mo.upload(req)
		assert.Nil(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		assert.Nil(t, err)
		assert.Equal(t, 200, data.StatusCode)
	})

	t.Run("Fail upload object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutFileFn: func(input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
					return nil, fmt.Errorf("error while creating object")
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

		_, err := mo.upload(req)
		assert.NotNil(t, err)
	})
}

func TestGetOperation(t *testing.T) {
	t.Run("Successfully get object", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
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

		_, err := mo.get(req)
		assert.Nil(t, err)
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

		_, err := mo.get(req)
		assert.NotNil(t, err)
	})

	t.Run("Fail get object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return nil, fmt.Errorf("error while getting object")
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

		_, err := mo.get(req)
		assert.NotNil(t, err)
	})

	t.Run("Fail get object with no response data", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
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

		_, err := mo.get(req)
		assert.NotNil(t, err)
	})
}

func TestDeleteOperation(t *testing.T) {
	t.Run("Successfully delete object", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				DeleteObjectFn: func(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
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

		out, err := mo.delete(req)
		assert.Nil(t, err)

		var data createResponse
		err = json.Unmarshal(out.Data, &data)
		assert.Nil(t, err)
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

		_, err := mo.delete(req)
		assert.NotNil(t, err)
	})

	t.Run("Fail delete object with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				DeleteObjectFn: func(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
					return nil, fmt.Errorf("error while deleting object")
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

		_, err := mo.delete(req)
		assert.NotNil(t, err)
	})
}

func TestListOperation(t *testing.T) {
	t.Run("Successfully list objects", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
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

		_, err := mo.list(req)
		assert.Nil(t, err)
	})

	t.Run("Fail list objects with obs internal error", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
					return nil, fmt.Errorf("error while listing objects")
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

		_, err := mo.list(req)
		assert.NotNil(t, err)
	})

	t.Run("Successfully list objects with default maxResults", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
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

		_, err := mo.list(req)
		assert.Nil(t, err)
	})
}

func TestInvoke(t *testing.T) {
	t.Run("Successfully invoke create", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				PutObjectFn: func(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
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

		_, err := mo.Invoke(context.TODO(), req)
		assert.Nil(t, err)
	})

	t.Run("Successfully invoke get", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				GetObjectFn: func(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
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

		_, err := mo.Invoke(context.TODO(), req)
		assert.Nil(t, err)
	})

	t.Run("Successfully invoke delete", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				DeleteObjectFn: func(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
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

		_, err := mo.Invoke(context.TODO(), req)
		assert.Nil(t, err)
	})

	t.Run("Successfully invoke list", func(t *testing.T) {
		mo := &HuaweiOBS{
			service: &MockHuaweiOBSService{
				ListObjectsFn: func(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
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

		_, err := mo.Invoke(context.TODO(), req)
		assert.Nil(t, err)
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

		_, err := mo.Invoke(context.TODO(), req)
		assert.NotNil(t, err)
	})
}
