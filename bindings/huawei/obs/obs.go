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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"github.com/google/uuid"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	metadataKey = "key"
	maxResults  = 1000
)

// add operations that are not listed under the standard bindings operations.
const (
	UploadOperation bindings.OperationKind = "upload"
)

type HuaweiOBS struct {
	metadata *obsMetadata
	service  HuaweiOBSAPI
	logger   logger.Logger
}

type obsMetadata struct {
	Region    string `json:"region"`    // (optional) the specific Huawei region of the bucket
	Endpoint  string `json:"endpoint"`  // the specific Huawei OBS endpoint
	AccessKey string `json:"accessKey"` // the Huawei Access Key (AK) to access the obs
	SecretKey string `json:"secretKey"` // the Huawei Secret Key (SK) to access the obs
	Bucket    string `json:"bucket"`    // the name of the Huawei OBS bucket to write to
}

type createResponse struct {
	StatusCode int    `json:"statusCode"`
	VersionID  string `json:"versionId"`
}

type uploadPayload struct {
	SourceFile string `json:"sourceFile"`
}

type listPayload struct {
	Marker     string `json:"marker"`
	Prefix     string `json:"prefix"`
	MaxResults int32  `json:"maxResults"`
	Delimiter  string `json:"delimiter"`
}

// NewHuaweiOBS returns a new Huawei OBS instance.
func NewHuaweiOBS(logger logger.Logger) bindings.OutputBinding {
	return &HuaweiOBS{logger: logger}
}

// Init does metadata parsing and connection creation.
func (o *HuaweiOBS) Init(_ context.Context, metadata bindings.Metadata) error {
	o.logger.Debugf("initializing Huawei OBS binding and parsing metadata")

	m, err := o.parseMetadata(metadata)
	if err != nil {
		return err
	}
	client, err := obs.New(m.AccessKey, m.SecretKey, m.Endpoint)
	if err != nil {
		return err
	}
	o.metadata = m
	o.service = &HuaweiOBSService{client: client}

	return nil
}

func (o *HuaweiOBS) parseMetadata(meta bindings.Metadata) (*obsMetadata, error) {
	var m obsMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.Bucket == "" {
		return nil, errors.New("missing obs bucket name")
	}
	if m.Endpoint == "" {
		return nil, errors.New("missing obs endpoint")
	}
	if m.AccessKey == "" {
		return nil, errors.New("missing the huawei access key")
	}
	if m.SecretKey == "" {
		return nil, errors.New("missing the huawei secret key")
	}

	o.logger.Debugf("Huawei OBS metadata=[%s]", m)
	return &m, nil
}

func (o *HuaweiOBS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		UploadOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (o *HuaweiOBS) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		o.logger.Debugf("key not found. generating key %s", key)
	}

	r := bytes.NewReader(req.Data)

	input := &obs.PutObjectInput{}
	input.Key = key
	input.Bucket = o.metadata.Bucket
	input.Body = r

	out, err := o.service.PutObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("obs binding error. putobject: %w", err)
	}

	jsonResponse, err := json.Marshal(createResponse{
		StatusCode: out.StatusCode,
		VersionID:  out.VersionId,
	})
	if err != nil {
		return nil, fmt.Errorf("obs binding error. error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (o *HuaweiOBS) upload(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload uploadPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		o.logger.Debugf("key not found. generating key %s", key)
	}

	input := &obs.PutFileInput{}
	input.Key = key
	input.Bucket = o.metadata.Bucket
	input.SourceFile = payload.SourceFile

	out, err := o.service.PutFile(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("obs binding error. putfile: %w", err)
	}

	jsonResponse, err := json.Marshal(createResponse{
		StatusCode: out.StatusCode,
		VersionID:  out.VersionId,
	})
	if err != nil {
		return nil, fmt.Errorf("obs binding error. error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (o *HuaweiOBS) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("obs binding error: can't read key value")
	}

	input := &obs.GetObjectInput{}
	input.Bucket = o.metadata.Bucket
	input.Key = key

	out, err := o.service.GetObject(ctx, input)
	if err != nil {
		var obsErr obs.ObsError
		if errors.As(err, &obsErr) && obsErr.StatusCode == http.StatusNotFound {
			return nil, errors.New("object not found")
		}
		return nil, fmt.Errorf("obs binding error. error getting obs object: %w", err)
	}

	// close connection at the end of operation
	defer func() {
		err = out.Body.Close()
		if err != nil {
			panic(err)
		}
	}()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("obs binding error. error reading obs object content: %w", err)
	}

	return &bindings.InvokeResponse{
		Data:     data,
		Metadata: nil,
	}, nil
}

func (o *HuaweiOBS) delete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var key string
	if val, ok := req.Metadata[metadataKey]; ok && val != "" {
		key = val
	} else {
		return nil, errors.New("obs binding error: can't read key value")
	}

	input := &obs.DeleteObjectInput{}
	input.Bucket = o.metadata.Bucket
	input.Key = key

	out, err := o.service.DeleteObject(ctx, input)
	if err != nil {
		var obsErr obs.ObsError
		if errors.As(err, &obsErr) && obsErr.StatusCode == http.StatusNotFound {
			return nil, errors.New("object not found")
		}
		return nil, fmt.Errorf("obs binding error. error deleting obs object: %w", err)
	}

	jsonResponse, err := json.Marshal(createResponse{
		StatusCode: out.StatusCode,
		VersionID:  out.VersionId,
	})
	if err != nil {
		return nil, fmt.Errorf("obs binding error. error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (o *HuaweiOBS) list(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload listPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.MaxResults == int32(0) {
		payload.MaxResults = maxResults
	}

	input := &obs.ListObjectsInput{}

	input.Bucket = o.metadata.Bucket
	input.MaxKeys = int(payload.MaxResults)
	input.Marker = payload.Marker
	input.Prefix = payload.Prefix
	input.Delimiter = payload.Delimiter

	out, err := o.service.ListObjects(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("obs binding error. error listing obs objects: %w", err)
	}

	jsonResponse, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("obs binding error. list operation. cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (o *HuaweiOBS) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return o.create(ctx, req)
	case UploadOperation:
		return o.upload(ctx, req)
	case bindings.GetOperation:
		return o.get(ctx, req)
	case bindings.DeleteOperation:
		return o.delete(ctx, req)
	case bindings.ListOperation:
		return o.list(ctx, req)
	default:
		return nil, fmt.Errorf("obs binding error. unsupported operation %s", req.Operation)
	}
}

// GetComponentMetadata returns the metadata of the component.
func (o *HuaweiOBS) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := obsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (o *HuaweiOBS) Close() error {
	if o.service != nil {
		o.service.Close()
	}
	return nil
}
