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

package s3

import (
	b64 "encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

// fakeS3Server creates an httptest server that implements minimal S3 GET/PUT/POST/DELETE.
// Objects are stored in memory keyed by path.
func fakeS3Server(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.RWMutex
	objects := make(map[string][]byte)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path // e.g. "/bucket/key"

		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			objects[key] = body
			mu.Unlock()
			w.Header().Set("Location", r.URL.String())
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			mu.RLock()
			data, ok := objects[key]
			mu.RUnlock()
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				//nolint:errcheck
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
				return
			}
			w.WriteHeader(http.StatusOK)
			//nolint:errcheck
			w.Write(data)

		case http.MethodDelete:
			mu.Lock()
			delete(objects, key)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		case http.MethodPost:
			// Handle DeleteObjects (batch delete) - path ends with ?delete
			if strings.Contains(r.URL.RawQuery, "delete") {
				// Parse the request body to find keys and actually delete them
				body, _ := io.ReadAll(r.Body)
				bodyStr := string(body)
				mu.Lock()
				// Extract keys from XML like <Key>foo</Key>
				for {
					start := strings.Index(bodyStr, "<Key>")
					if start == -1 {
						break
					}
					end := strings.Index(bodyStr[start:], "</Key>")
					if end == -1 {
						break
					}
					delKey := bodyStr[start+5 : start+end]
					// The key in the store includes the bucket prefix
					delete(objects, "/test/"+delKey)
					bodyStr = bodyStr[start+end+6:]
				}
				mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				//nolint:errcheck
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`))
				return
			}
			// Handle multipart upload initiation
			if strings.Contains(r.URL.RawQuery, "uploads") {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				//nolint:errcheck
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>test</Bucket><Key>` + key + `</Key><UploadId>fake-upload-id</UploadId></InitiateMultipartUploadResult>`))
				return
			}
			// Handle multipart upload completion
			if strings.Contains(r.URL.RawQuery, "uploadId") {
				// For complete multipart, read the body (it's the part list XML)
				// but we already stored data via PUT for each part
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				//nolint:errcheck
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Location>` + r.URL.String() + `</Location><Bucket>test</Bucket><Key>` + key + `</Key></CompleteMultipartUploadResult>`))
				return
			}
			http.Error(w, "unsupported POST", http.StatusBadRequest)

		default:
			http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		}
	}))
}

// newTestAWSS3 creates an AWSS3 instance pointed at the given fake server endpoint.
func newTestAWSS3(t *testing.T, endpoint string) *AWSS3 {
	t.Helper()
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		BaseEndpoint: &endpoint,
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return &AWSS3{
		metadata:  &s3Metadata{Bucket: "test", ForcePathStyle: true, Endpoint: endpoint},
		s3Client:  s3Client,
		tmClient:  transfermanager.New(s3Client),
		presigner: s3.NewPresignClient(s3Client),
		logger:    logger.NewLogger("s3-test"),
	}
}

func TestParseMetadata(t *testing.T) {
	t.Run("Has correct metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "yes",
			"DisableSSL":     "true",
			"InsecureSSL":    "1",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)

		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)
		assert.True(t, meta.DisableSSL)
		assert.True(t, meta.InsecureSSL)
	})
}

func TestParseS3Tags(t *testing.T) {
	t.Run("Has parsed s3 tags", func(t *testing.T) {
		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "yes",
			"encodeBase64": "false",
			"filePath":     "/usr/vader.darth",
			"storageClass": "STANDARD_IA",
			"tags":         "project=myproject,year=2024",
		}
		s3 := AWSS3{}
		parsedTags, err := s3.parseS3Tags(request.Metadata["tags"])

		require.NoError(t, err)
		assert.Equal(t, "project=myproject&year=2024", *parsedTags)
	})
}

func TestMergeWithRequestMetadata(t *testing.T) {
	t.Run("Has merged metadata", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "YES",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "yes",
			"encodeBase64": "false",
			"filePath":     "/usr/vader.darth",
			"presignTTL":   "15s",
			"storageClass": "STANDARD_IA",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.Equal(t, "key", mergedMeta.AccessKey)
		assert.Equal(t, "region", mergedMeta.Region)
		assert.Equal(t, "secret", mergedMeta.SecretKey)
		assert.Equal(t, "test", mergedMeta.Bucket)
		assert.Equal(t, "endpoint", mergedMeta.Endpoint)
		assert.Equal(t, "token", mergedMeta.SessionToken)
		assert.True(t, meta.ForcePathStyle)
		assert.True(t, mergedMeta.DecodeBase64)
		assert.False(t, mergedMeta.EncodeBase64)
		assert.Equal(t, "/usr/vader.darth", mergedMeta.FilePath)
		assert.Equal(t, "15s", mergedMeta.PresignTTL)
		assert.Equal(t, "STANDARD_IA", mergedMeta.StorageClass)
	})

	t.Run("Has invalid merged metadata decodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "true",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"decodeBase64": "hello",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.False(t, mergedMeta.DecodeBase64)
	})

	t.Run("Has invalid merged metadata encodeBase64", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{
			"AccessKey":      "key",
			"Region":         "region",
			"SecretKey":      "secret",
			"Bucket":         "test",
			"Endpoint":       "endpoint",
			"SessionToken":   "token",
			"ForcePathStyle": "true",
		}
		s3 := AWSS3{}
		meta, err := s3.parseMetadata(m)
		require.NoError(t, err)
		assert.Equal(t, "key", meta.AccessKey)
		assert.Equal(t, "region", meta.Region)
		assert.Equal(t, "secret", meta.SecretKey)
		assert.Equal(t, "test", meta.Bucket)
		assert.Equal(t, "endpoint", meta.Endpoint)
		assert.Equal(t, "token", meta.SessionToken)
		assert.True(t, meta.ForcePathStyle)

		request := bindings.InvokeRequest{}
		request.Metadata = map[string]string{
			"encodeBase64": "bye",
		}

		mergedMeta, err := meta.mergeWithRequestMetadata(&request)

		require.NoError(t, err)
		assert.False(t, mergedMeta.EncodeBase64)
	})
}

func TestGetOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s3.metadata = &s3Metadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.get(t.Context(), &r)
		require.Error(t, err)
	})
}

func TestDeleteOption(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s3.metadata = &s3Metadata{}

	t.Run("return error if key is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := s3.delete(t.Context(), &r)
		require.Error(t, err)
	})
}

func TestInitCreatesV2Clients(t *testing.T) {
	s3 := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)

	md := bindings.Metadata{}
	md.Properties = map[string]string{
		"bucket": "test-bucket",
		"region": "us-west-2",
	}

	reqCtx := t.Context()
	err := s3.Init(reqCtx, md)
	require.NoError(t, err)
	require.NotNil(t, s3.s3Client)
	require.NotNil(t, s3.tmClient)
	require.NotNil(t, s3.presigner)
}

func TestForcePathStylePresignURL(t *testing.T) {
	bucket := "dapr-s3-test"
	key := "filename.txt"
	region := "us-east-1"

	cfg := aws.Config{
		Region:      region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
	}

	t.Run("forcePathStyle=false", func(t *testing.T) {
		s3Client := s3.NewFromConfig(cfg)
		presignClient := s3.NewPresignClient(s3Client)
		presigned, err := presignClient.PresignGetObject(t.Context(), &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		require.NoError(t, err)
		require.Contains(t, presigned.URL, ".s3.")
		require.Contains(t, presigned.URL, bucket)
		require.Contains(t, presigned.URL, key)
		require.Equal(t, "https://"+bucket+".s3."+region+".amazonaws.com/"+key, presigned.URL[:len("https://"+bucket+".s3."+region+".amazonaws.com/"+key)])
	})

	t.Run("forcePathStyle=true", func(t *testing.T) {
		s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})
		presignClient := s3.NewPresignClient(s3Client)
		presigned, err := presignClient.PresignGetObject(t.Context(), &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		require.NoError(t, err)
		require.Contains(t, presigned.URL, "/"+bucket+"/"+key)
		require.Equal(t, "https://s3."+region+".amazonaws.com/"+bucket+"/"+key, presigned.URL[:len("https://s3."+region+".amazonaws.com/"+bucket+"/"+key)])
	})
}

func TestOperationsIncludesBulk(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	ops := s.Operations()

	opNames := make(map[bindings.OperationKind]bool)
	for _, op := range ops {
		opNames[op] = true
	}

	assert.True(t, opNames[bulkGetOperation], "should include bulkGet")
	assert.True(t, opNames[bulkCreateOperation], "should include bulkCreate")
	assert.True(t, opNames[bulkDeleteOperation], "should include bulkDelete")
}

func TestBulkGetValidation(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s.metadata = &s3Metadata{Bucket: "test"}

	t.Run("invalid JSON payload", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data:     []byte("not-json"),
			Metadata: map[string]string{},
		}
		_, err := s.bulkGet(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid bulkGet payload")
	})

	t.Run("empty items", func(t *testing.T) {
		payload, _ := json.Marshal(bulkGetPayload{Items: []bulkGetItem{}})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		_, err := s.bulkGet(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least one item")
	})

	t.Run("nil items", func(t *testing.T) {
		payload, _ := json.Marshal(bulkGetPayload{})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		_, err := s.bulkGet(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least one item")
	})

	t.Run("concurrency zero is error", func(t *testing.T) {
		payload, _ := json.Marshal(bulkGetPayload{
			Items:       []bulkGetItem{{Key: "a"}},
			Concurrency: ptr.Of(0),
		})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		_, err := s.bulkGet(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "concurrency must be greater than 0")
	})

	t.Run("negative concurrency is error", func(t *testing.T) {
		payload, _ := json.Marshal(bulkGetPayload{
			Items:       []bulkGetItem{{Key: "a"}},
			Concurrency: ptr.Of(-1),
		})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		_, err := s.bulkGet(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "concurrency must be greater than 0")
	})

	t.Run("empty key in item returns per-item error", func(t *testing.T) {
		payload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{{Key: ""}},
		})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		resp, err := s.bulkGet(t.Context(), &r)
		require.NoError(t, err)
		var results []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &results))
		require.Len(t, results, 1)
		assert.Equal(t, "key is required", results[0].Error)
	})
}

func TestBulkCreateValidation(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s.metadata = &s3Metadata{Bucket: "test"}

	t.Run("invalid JSON payload", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data:     []byte("{bad}"),
			Metadata: map[string]string{},
		}
		_, err := s.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid bulkCreate payload")
	})

	t.Run("empty items", func(t *testing.T) {
		payload, _ := json.Marshal(bulkCreatePayload{Items: []bulkCreateItem{}})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		_, err := s.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least one item")
	})

	t.Run("concurrency zero is error", func(t *testing.T) {
		payload, _ := json.Marshal(bulkCreatePayload{
			Items:       []bulkCreateItem{{Key: "a", Data: ptr.Of("x")}},
			Concurrency: ptr.Of(0),
		})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		_, err := s.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "concurrency must be greater than 0")
	})
}

func TestBulkDeleteValidation(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s.metadata = &s3Metadata{Bucket: "test"}

	t.Run("invalid JSON payload", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data:     []byte("bad"),
			Metadata: map[string]string{},
		}
		_, err := s.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid bulkDelete payload")
	})

	t.Run("empty keys", func(t *testing.T) {
		payload, _ := json.Marshal(bulkDeletePayload{Keys: []string{}})
		r := bindings.InvokeRequest{
			Data: payload,
		}
		_, err := s.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires at least one key")
	})

	t.Run("empty key in list returns per-item error", func(t *testing.T) {
		srv := fakeS3Server(t)
		defer srv.Close()
		s := newTestAWSS3(t, srv.URL)

		payload, _ := json.Marshal(bulkDeletePayload{Keys: []string{"valid.txt", "", "  "}})
		r := bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		}
		resp, err := s.bulkDelete(t.Context(), &r)
		require.NoError(t, err)
		var results []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &results))
		require.Len(t, results, 3)
		assert.Empty(t, results[0].Error, "valid key should succeed")
		assert.Equal(t, "key is required", results[1].Error)
		assert.Equal(t, "key is required", results[2].Error)
	})
}

func TestBulkPayloadParsing(t *testing.T) {
	t.Run("bulkGet payload with concurrency and filePath", func(t *testing.T) {
		raw := `{"items":[{"key":"a.txt"},{"key":"b.txt","filePath":"/tmp/b.txt"}],"concurrency":5}`
		var p bulkGetPayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		require.Len(t, p.Items, 2)
		assert.Equal(t, "a.txt", p.Items[0].Key)
		assert.Nil(t, p.Items[0].FilePath)
		assert.Equal(t, "b.txt", p.Items[1].Key)
		require.NotNil(t, p.Items[1].FilePath)
		assert.Equal(t, "/tmp/b.txt", *p.Items[1].FilePath)
		require.NotNil(t, p.Concurrency)
		assert.Equal(t, 5, *p.Concurrency)
	})

	t.Run("bulkGet payload without concurrency is nil", func(t *testing.T) {
		raw := `{"items":[{"key":"a.txt"}]}`
		var p bulkGetPayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Nil(t, p.Concurrency)
	})

	t.Run("bulkCreate payload with pointers", func(t *testing.T) {
		raw := `{"items":[{"key":"f1.txt","data":"hello"},{"key":"f2.txt","filePath":"/tmp/f2.txt","contentType":"text/plain"}],"concurrency":3}`
		var p bulkCreatePayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Len(t, p.Items, 2)
		assert.Equal(t, "f1.txt", p.Items[0].Key)
		require.NotNil(t, p.Items[0].Data)
		assert.Equal(t, "hello", *p.Items[0].Data)
		assert.Nil(t, p.Items[0].FilePath)
		assert.Nil(t, p.Items[0].ContentType)
		assert.Equal(t, "f2.txt", p.Items[1].Key)
		require.NotNil(t, p.Items[1].FilePath)
		assert.Equal(t, "/tmp/f2.txt", *p.Items[1].FilePath)
		require.NotNil(t, p.Items[1].ContentType)
		assert.Equal(t, "text/plain", *p.Items[1].ContentType)
		require.NotNil(t, p.Concurrency)
		assert.Equal(t, 3, *p.Concurrency)
	})

	t.Run("bulkCreate item with neither data nor filePath", func(t *testing.T) {
		raw := `{"items":[{"key":"f1.txt"}]}`
		var p bulkCreatePayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Nil(t, p.Items[0].Data)
		assert.Nil(t, p.Items[0].FilePath)
	})

	t.Run("bulkDelete payload", func(t *testing.T) {
		raw := `{"keys":["x.txt","y.txt"]}`
		var p bulkDeletePayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Equal(t, []string{"x.txt", "y.txt"}, p.Keys)
	})
}

func TestBulkCreateMissingKey(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s.metadata = &s3Metadata{Bucket: "test"}

	// Test the validation path with only the empty-key item.
	payload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "", Data: ptr.Of("data1")},
		},
	})
	r := bindings.InvokeRequest{
		Data:     payload,
		Metadata: map[string]string{},
	}
	resp, err := s.bulkCreate(t.Context(), &r)
	require.NoError(t, err)

	var results []bulkItemResult
	err = json.Unmarshal(resp.Data, &results)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "key is required", results[0].Error)
}

func TestBulkCreateMissingDataAndFilePath(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s.metadata = &s3Metadata{Bucket: "test"}

	// Item with key but neither data nor filePath
	payload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "test.txt"},
		},
	})
	r := bindings.InvokeRequest{
		Data:     payload,
		Metadata: map[string]string{},
	}
	resp, err := s.bulkCreate(t.Context(), &r)
	require.NoError(t, err)

	var results []bulkItemResult
	err = json.Unmarshal(resp.Data, &results)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "either data or filePath is required", results[0].Error)
}

func TestResolveConcurrency(t *testing.T) {
	t.Run("nil returns default", func(t *testing.T) {
		c, err := resolveConcurrency(nil)
		require.NoError(t, err)
		assert.Equal(t, defaultBulkConcurrency, c)
	})

	t.Run("positive value is returned", func(t *testing.T) {
		c, err := resolveConcurrency(ptr.Of(25))
		require.NoError(t, err)
		assert.Equal(t, 25, c)
	})

	t.Run("zero is error", func(t *testing.T) {
		_, err := resolveConcurrency(ptr.Of(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "concurrency must be greater than 0")
	})

	t.Run("negative is error", func(t *testing.T) {
		_, err := resolveConcurrency(ptr.Of(-5))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "concurrency must be greater than 0")
	})
}

func TestBulkDeleteChunking(t *testing.T) {
	// Verify the chunking logic by checking that maxDeleteBatchSize is 1000
	assert.Equal(t, 1000, maxDeleteBatchSize)

	// Verify that bulkDeletePayload can hold more than 1000 keys
	keys := make([]string, 2500)
	for i := range keys {
		keys[i] = "key" + string(rune('0'+i%10))
	}
	p := bulkDeletePayload{Keys: keys}
	data, err := json.Marshal(p)
	require.NoError(t, err)

	var parsed bulkDeletePayload
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Len(t, parsed.Keys, 2500)
}

func TestBulkItemResultJSON(t *testing.T) {
	t.Run("omits empty fields", func(t *testing.T) {
		r := bulkItemResult{Key: "test.txt"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "data")
		assert.NotContains(t, string(data), "location")
		assert.NotContains(t, string(data), "error")
		assert.Contains(t, string(data), `"key":"test.txt"`)
	})

	t.Run("includes error when present", func(t *testing.T) {
		r := bulkItemResult{Key: "test.txt", Error: "not found"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"error":"not found"`)
	})

	t.Run("includes data when present", func(t *testing.T) {
		r := bulkItemResult{Key: "test.txt", Data: "hello"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"data"`)
	})

	t.Run("includes location when present", func(t *testing.T) {
		r := bulkItemResult{Key: "test.txt", Location: "https://bucket.s3.amazonaws.com/test.txt"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"location"`)
	})
}

func TestInvokeRoutesBulkOperations(t *testing.T) {
	s := NewAWSS3(logger.NewLogger("s3")).(*AWSS3)
	s.metadata = &s3Metadata{Bucket: "test"}

	// Use empty/invalid payloads to trigger validation errors (not "unsupported operation")
	// which proves the operation was routed to the correct handler.
	t.Run("bulkGet routes correctly", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Operation: bulkGetOperation,
			Data:      []byte(`{"items":[]}`),
			Metadata:  map[string]string{},
		}
		_, err := s.Invoke(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bulkGet requires at least one item")
	})

	t.Run("bulkCreate routes correctly", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Operation: bulkCreateOperation,
			Data:      []byte(`{"items":[]}`),
			Metadata:  map[string]string{},
		}
		_, err := s.Invoke(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bulkCreate requires at least one item")
	})

	t.Run("bulkDelete routes correctly", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Operation: bulkDeleteOperation,
			Data:      []byte(`{"keys":[]}`),
			Metadata:  map[string]string{},
		}
		_, err := s.Invoke(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bulkDelete requires at least one key")
	})

	t.Run("unknown operation returns error", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Operation: "nonexistent",
			Data:      []byte(`{}`),
			Metadata:  map[string]string{},
		}
		_, err := s.Invoke(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported operation")
	})
}

func TestBulkCreateAndGetRoundTrip(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	t.Run("raw data round-trip", func(t *testing.T) {
		// Create two objects
		payload, _ := json.Marshal(bulkCreatePayload{
			Items: []bulkCreateItem{
				{Key: "hello.txt", Data: ptr.Of("hello world")},
				{Key: "foo.txt", Data: ptr.Of("foo bar baz")},
			},
		})
		resp, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
			Data:     payload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)
		var createResults []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &createResults))
		require.Len(t, createResults, 2)
		for _, r := range createResults {
			assert.Empty(t, r.Error)
			assert.NotEmpty(t, r.Location)
		}

		// Get them back
		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{
				{Key: "hello.txt"},
				{Key: "foo.txt"},
			},
		})
		resp, err = s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)
		var getResults []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &getResults))
		require.Len(t, getResults, 2)

		expected := map[string]string{
			"hello.txt": "hello world",
			"foo.txt":   "foo bar baz",
		}
		for _, r := range getResults {
			assert.Empty(t, r.Error)
			assert.Equal(t, expected[r.Key], r.Data)
		}
	})
}

func TestBulkGetEncodeBase64(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	// Upload raw data
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "binary.bin", Data: ptr.Of("binary content here")},
		},
	})
	_, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)

	t.Run("encodeBase64 returns base64 encoded data", func(t *testing.T) {
		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{{Key: "binary.bin"}},
		})
		resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{"encodeBase64": "true"},
		})
		require.NoError(t, err)

		var results []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &results))
		require.Len(t, results, 1)
		assert.Empty(t, results[0].Error)

		// Verify the data is valid base64 and decodes to original
		decoded, err := b64.StdEncoding.DecodeString(results[0].Data)
		require.NoError(t, err)
		assert.Equal(t, "binary content here", string(decoded))
	})

	t.Run("without encodeBase64 returns raw data", func(t *testing.T) {
		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{{Key: "binary.bin"}},
		})
		resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)

		var results []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &results))
		require.Len(t, results, 1)
		assert.Empty(t, results[0].Error)
		assert.Equal(t, "binary content here", results[0].Data)
	})
}

func TestBulkCreateDecodeBase64(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	originalContent := "hello base64 world"
	b64Content := b64.StdEncoding.EncodeToString([]byte(originalContent))

	// Upload with decodeBase64 = true (data is base64, should be decoded before storing)
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "decoded.txt", Data: ptr.Of(b64Content)},
		},
	})
	_, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{"decodeBase64": "true"},
	})
	require.NoError(t, err)

	// Get back raw - should be the decoded original content
	getPayload, _ := json.Marshal(bulkGetPayload{
		Items: []bulkGetItem{{Key: "decoded.txt"}},
	})
	resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
		Data:     getPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)

	var results []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &results))
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Error)
	assert.Equal(t, originalContent, results[0].Data)
}

func TestBulkCreateDecodeBase64ThenGetEncodeBase64(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	originalContent := "round trip base64"
	b64Content := b64.StdEncoding.EncodeToString([]byte(originalContent))

	// Upload: base64 data → decode before store
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "roundtrip.txt", Data: ptr.Of(b64Content)},
		},
	})
	_, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{"decodeBase64": "true"},
	})
	require.NoError(t, err)

	// Get: encode back to base64
	getPayload, _ := json.Marshal(bulkGetPayload{
		Items: []bulkGetItem{{Key: "roundtrip.txt"}},
	})
	resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
		Data:     getPayload,
		Metadata: map[string]string{"encodeBase64": "true"},
	})
	require.NoError(t, err)

	var results []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &results))
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Error)

	// The returned data should be base64 of the original content
	assert.Equal(t, b64Content, results[0].Data)
}

func TestBulkGetToFile(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	// Upload some content
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "download-me.txt", Data: ptr.Of("file content for download")},
		},
	})
	_, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)

	t.Run("stream to file raw", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "out.txt")

		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{
				{Key: "download-me.txt", FilePath: ptr.Of(outPath)},
			},
		})
		resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)

		var results []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &results))
		require.Len(t, results, 1)
		assert.Empty(t, results[0].Error)
		assert.Empty(t, results[0].Data, "data should be empty when writing to file")

		// Verify file contents
		fileBytes, err := os.ReadFile(outPath)
		require.NoError(t, err)
		assert.Equal(t, "file content for download", string(fileBytes))
	})

	t.Run("stream to file with encodeBase64", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "out-b64.txt")

		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{
				{Key: "download-me.txt", FilePath: ptr.Of(outPath)},
			},
		})
		resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{"encodeBase64": "true"},
		})
		require.NoError(t, err)

		var results []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &results))
		require.Len(t, results, 1)
		assert.Empty(t, results[0].Error)

		// Verify file contains base64-encoded content
		fileBytes, err := os.ReadFile(outPath)
		require.NoError(t, err)
		decoded, err := b64.StdEncoding.DecodeString(string(fileBytes))
		require.NoError(t, err)
		assert.Equal(t, "file content for download", string(decoded))
	})
}

func TestBulkCreateFromFile(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	t.Run("upload from filePath", func(t *testing.T) {
		tmpDir := t.TempDir()
		srcPath := filepath.Join(tmpDir, "upload.txt")
		require.NoError(t, os.WriteFile(srcPath, []byte("uploaded from file"), 0o600))

		createPayload, _ := json.Marshal(bulkCreatePayload{
			Items: []bulkCreateItem{
				{Key: "from-file.txt", FilePath: ptr.Of(srcPath)},
			},
		})
		resp, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
			Data:     createPayload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)

		var createResults []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &createResults))
		require.Len(t, createResults, 1)
		assert.Empty(t, createResults[0].Error)

		// Verify by getting it back
		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{{Key: "from-file.txt"}},
		})
		resp, err = s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)

		var getResults []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &getResults))
		require.Len(t, getResults, 1)
		assert.Empty(t, getResults[0].Error)
		assert.Equal(t, "uploaded from file", getResults[0].Data)
	})

	t.Run("upload from filePath with decodeBase64", func(t *testing.T) {
		tmpDir := t.TempDir()
		srcPath := filepath.Join(tmpDir, "upload-b64.txt")
		original := "base64 file content"
		require.NoError(t, os.WriteFile(srcPath, []byte(b64.StdEncoding.EncodeToString([]byte(original))), 0o600))

		createPayload, _ := json.Marshal(bulkCreatePayload{
			Items: []bulkCreateItem{
				{Key: "from-b64-file.txt", FilePath: ptr.Of(srcPath)},
			},
		})
		resp, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
			Data:     createPayload,
			Metadata: map[string]string{"decodeBase64": "true"},
		})
		require.NoError(t, err)

		var createResults []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &createResults))
		require.Len(t, createResults, 1)
		assert.Empty(t, createResults[0].Error)

		// Verify by getting raw - should be decoded content
		getPayload, _ := json.Marshal(bulkGetPayload{
			Items: []bulkGetItem{{Key: "from-b64-file.txt"}},
		})
		resp, err = s.bulkGet(t.Context(), &bindings.InvokeRequest{
			Data:     getPayload,
			Metadata: map[string]string{},
		})
		require.NoError(t, err)

		var getResults []bulkItemResult
		require.NoError(t, json.Unmarshal(resp.Data, &getResults))
		require.Len(t, getResults, 1)
		assert.Empty(t, getResults[0].Error)
		assert.Equal(t, original, getResults[0].Data)
	})
}

func TestBulkGetPartialFailure(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	// Upload one object
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "exists.txt", Data: ptr.Of("I exist")},
		},
	})
	_, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)

	// Get one that exists and one that doesn't
	getPayload, _ := json.Marshal(bulkGetPayload{
		Items: []bulkGetItem{
			{Key: "exists.txt"},
			{Key: "nope.txt"},
		},
	})
	resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
		Data:     getPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)

	var results []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &results))
	require.Len(t, results, 2)

	for _, r := range results {
		if r.Key == "exists.txt" {
			assert.Empty(t, r.Error)
			assert.Equal(t, "I exist", r.Data)
		} else {
			assert.NotEmpty(t, r.Error, "non-existent key should have an error")
			assert.Empty(t, r.Data)
		}
	}
}

func TestBulkDeleteRoundTrip(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	// Create objects
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items: []bulkCreateItem{
			{Key: "del-a.txt", Data: ptr.Of("a")},
			{Key: "del-b.txt", Data: ptr.Of("b")},
		},
	})
	_, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)

	// Verify they exist
	getPayload, _ := json.Marshal(bulkGetPayload{
		Items: []bulkGetItem{{Key: "del-a.txt"}, {Key: "del-b.txt"}},
	})
	resp, err := s.bulkGet(t.Context(), &bindings.InvokeRequest{
		Data:     getPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	var getResults []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &getResults))
	for _, r := range getResults {
		assert.Empty(t, r.Error)
	}

	// Bulk delete
	deletePayload, _ := json.Marshal(bulkDeletePayload{
		Keys: []string{"del-a.txt", "del-b.txt"},
	})
	resp, err = s.bulkDelete(t.Context(), &bindings.InvokeRequest{
		Data:     deletePayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	var deleteResults []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &deleteResults))
	require.Len(t, deleteResults, 2)
	for _, r := range deleteResults {
		assert.Empty(t, r.Error)
	}

	// Verify they're gone
	resp, err = s.bulkGet(t.Context(), &bindings.InvokeRequest{
		Data:     getPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resp.Data, &getResults))
	for _, r := range getResults {
		assert.NotEmpty(t, r.Error, "deleted key %s should have error", r.Key)
	}
}

func TestBulkMultipleConcurrency(t *testing.T) {
	srv := fakeS3Server(t)
	defer srv.Close()
	s := newTestAWSS3(t, srv.URL)

	// Create 20 objects with concurrency=3
	items := make([]bulkCreateItem, 20)
	for i := range items {
		key := "concurrent-" + string(rune('a'+i)) + ".txt"
		data := "data-" + string(rune('a'+i))
		items[i] = bulkCreateItem{Key: key, Data: ptr.Of(data)}
	}
	createPayload, _ := json.Marshal(bulkCreatePayload{
		Items:       items,
		Concurrency: ptr.Of(3),
	})
	resp, err := s.bulkCreate(t.Context(), &bindings.InvokeRequest{
		Data:     createPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	var createResults []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &createResults))
	require.Len(t, createResults, 20)
	for _, r := range createResults {
		assert.Empty(t, r.Error, "key %s should succeed", r.Key)
	}

	// Get all 20 with concurrency=5
	getItems := make([]bulkGetItem, 20)
	for i := range getItems {
		getItems[i] = bulkGetItem{Key: items[i].Key}
	}
	getPayload, _ := json.Marshal(bulkGetPayload{
		Items:       getItems,
		Concurrency: ptr.Of(5),
	})
	resp, err = s.bulkGet(t.Context(), &bindings.InvokeRequest{
		Data:     getPayload,
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	var getResults []bulkItemResult
	require.NoError(t, json.Unmarshal(resp.Data, &getResults))
	require.Len(t, getResults, 20)
	for _, r := range getResults {
		assert.Empty(t, r.Error, "key %s should succeed", r.Key)
		assert.NotEmpty(t, r.Data)
	}
}
