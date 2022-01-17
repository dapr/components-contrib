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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/alibabacloud-go/darabonba-openapi/client"
	oos "github.com/alibabacloud-go/oos-20190601/client"
	"github.com/alibabacloud-go/tea/tea"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

// Constant literals.
const (
	VersionID = "version_id"
	Path      = "path"
)

// NewParameterStore returns a new oos parameter store.
func NewParameterStore(logger logger.Logger) secretstores.SecretStore {
	return &oosSecretStore{logger: logger}
}

type parameterStoreMetaData struct {
	RegionID        *string `json:"regionId"`
	AccessKeyID     *string `json:"accessKeyId"`
	AccessKeySecret *string `json:"accessKeySecret"`
	SecurityToken   *string `json:"securityToken"`
}

type parameterStoreClient interface {
	GetSecretParameter(request *oos.GetSecretParameterRequest) (*oos.GetSecretParameterResponse, error)
	GetSecretParametersByPath(request *oos.GetSecretParametersByPathRequest) (*oos.GetSecretParametersByPathResponse, error)
}

type oosSecretStore struct {
	client parameterStoreClient
	logger logger.Logger
}

// Init creates a Alicloud parameter store client.
func (o *oosSecretStore) Init(metadata secretstores.Metadata) error {
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
func (o *oosSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	name := req.Name

	parameterVersion, err := o.getVersionFromMetadata(req.Metadata)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	output, err := o.client.GetSecretParameter(&oos.GetSecretParameterRequest{
		Name:             tea.String(name),
		WithDecryption:   tea.Bool(true),
		ParameterVersion: parameterVersion,
	})
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
func (o *oosSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	response := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	path := o.getPathFromMetadata(req.Metadata)
	if path == nil {
		path = tea.String("/")
	}
	var nextToken *string

	for {
		output, err := o.client.GetSecretParametersByPath(&oos.GetSecretParametersByPathRequest{
			Path:           path,
			WithDecryption: tea.Bool(true),
			Recursive:      tea.Bool(true),
			NextToken:      nextToken,
		})
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

func (o *oosSecretStore) getClient(metadata *parameterStoreMetaData) (*oos.Client, error) {
	config := &client.Config{
		RegionId:        metadata.RegionID,
		AccessKeyId:     metadata.AccessKeyID,
		AccessKeySecret: metadata.AccessKeySecret,
		SecurityToken:   metadata.SecurityToken,
	}
	return oos.NewClient(config)
}

func (o *oosSecretStore) getParameterStoreMetadata(spec secretstores.Metadata) (*parameterStoreMetaData, error) {
	b, err := json.Marshal(spec.Properties)
	if err != nil {
		return nil, err
	}

	var meta parameterStoreMetaData
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
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
