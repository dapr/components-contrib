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

package parameterstore

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/alibabacloud-go/darabonba-openapi/client"
	oos "github.com/alibabacloud-go/oos-20190601/client"
	util "github.com/alibabacloud-go/tea-utils/service"
	"github.com/alibabacloud-go/tea/tea"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// Constant literals.
const (
	VersionID = "version_id"
	Path      = "path"
)

var _ secretstores.SecretStore = (*oosSecretStore)(nil)

// NewParameterStore returns a new oos parameter store.
func NewParameterStore(logger logger.Logger) secretstores.SecretStore {
	return &oosSecretStore{logger: logger}
}

type ParameterStoreMetaData struct {
	RegionID        *string `json:"regionId"`
	AccessKeyID     *string `json:"accessKeyId"`
	AccessKeySecret *string `json:"accessKeySecret"`
	SecurityToken   *string `json:"securityToken"`
}

type parameterStoreClient interface {
	GetSecretParameterWithOptions(request *oos.GetSecretParameterRequest, runtime *util.RuntimeOptions) (*oos.GetSecretParameterResponse, error)
	GetSecretParametersByPathWithOptions(request *oos.GetSecretParametersByPathRequest, runtime *util.RuntimeOptions) (*oos.GetSecretParametersByPathResponse, error)
}

type oosSecretStore struct {
	client parameterStoreClient
	logger logger.Logger
}

// Init creates a Alicloud parameter store client.
func (o *oosSecretStore) Init(_ context.Context, metadata secretstores.Metadata) error {
	meta, err := o.getParameterStoreMetadata(metadata)
	if err != nil {
		return err
	}

	client, err := o.getClient(meta)
	if err != nil {
		return err
	}
	o.client = client

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (o *oosSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	name := req.Name

	parameterVersion, err := o.getVersionFromMetadata(req.Metadata)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	runtime := &util.RuntimeOptions{}
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline).Milliseconds()
		runtime.SetReadTimeout(int(timeout))
	}
	output, err := o.client.GetSecretParameterWithOptions(&oos.GetSecretParameterRequest{
		Name:             tea.String(name),
		WithDecryption:   tea.Bool(true),
		ParameterVersion: parameterVersion,
	}, runtime)
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %w", err)
	}

	response := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	parameter := output.Body.Parameter
	if parameter != nil {
		response.Data[*parameter.Name] = *parameter.Value
	}

	return response, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (o *oosSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	response := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	path := o.getPathFromMetadata(req.Metadata)
	if path == nil {
		path = tea.String("/")
	}
	var nextToken *string

	for {
		runtime := &util.RuntimeOptions{}
		if deadline, ok := ctx.Deadline(); ok {
			timeout := time.Until(deadline).Milliseconds()
			runtime.SetReadTimeout(int(timeout))
		}
		output, err := o.client.GetSecretParametersByPathWithOptions(&oos.GetSecretParametersByPathRequest{
			Path:           path,
			WithDecryption: tea.Bool(true),
			Recursive:      tea.Bool(true),
			NextToken:      nextToken,
		}, runtime)
		if err != nil {
			return secretstores.BulkGetSecretResponse{}, fmt.Errorf("couldn't get secrets: %w", err)
		}

		parameters := output.Body.Parameters
		nextToken = output.Body.NextToken

		for _, parameter := range parameters {
			response.Data[*parameter.Name] = map[string]string{*parameter.Name: *parameter.Value}
		}
		if nextToken == nil {
			break
		}
	}

	return response, nil
}

func (o *oosSecretStore) getClient(metadata *ParameterStoreMetaData) (*oos.Client, error) {
	config := &client.Config{
		RegionId:        metadata.RegionID,
		AccessKeyId:     metadata.AccessKeyID,
		AccessKeySecret: metadata.AccessKeySecret,
		SecurityToken:   metadata.SecurityToken,
	}
	return oos.NewClient(config)
}

func (o *oosSecretStore) getParameterStoreMetadata(spec secretstores.Metadata) (*ParameterStoreMetaData, error) {
	meta := ParameterStoreMetaData{}
	err := kitmd.DecodeMetadata(spec.Properties, &meta)
	return &meta, err
}

// getVersionFromMetadata returns the parameter version from the metadata. If not set means latest version.
func (o *oosSecretStore) getVersionFromMetadata(metadata map[string]string) (*int32, error) {
	if s, ok := metadata[VersionID]; ok && s != "" {
		val, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		converted := int32(val)

		return &converted, nil
	}

	return nil, nil
}

// getPathFromMetadata returns the path from the metadata. If not set means root path (all secrets).
func (o *oosSecretStore) getPathFromMetadata(metadata map[string]string) *string {
	if s, ok := metadata[Path]; ok {
		return &s
	}

	return nil
}

// Features returns the features available in this secret store.
func (o *oosSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (o *oosSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := ParameterStoreMetaData{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (o *oosSecretStore) Close() error {
	return nil
}
