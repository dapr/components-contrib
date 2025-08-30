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

package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	awsCommon "github.com/dapr/components-contrib/common/aws"
	awsCommonAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	VersionID    = "version_id"
	VersionStage = "version_stage"
)

var _ secretstores.SecretStore = (*smSecretStore)(nil)

// NewSecretManager returns a new secret manager store.
func NewSecretManager(logger logger.Logger) secretstores.SecretStore {
	return &smSecretStore{logger: logger}
}

type SecretManagerMetaData struct {
	// Ignored by metadata parser because included in built-in authentication profile
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`

	// TODO: rm the alias in Dapr 1.17
	Region                     string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	Endpoint                   string `json:"endpoint" mapstructure:"endpoint"`
	MultipleKeyValuesPerSecret bool   `json:"multipleKeyValuesPerSecret" mapstructure:"multipleKeyValuesPerSecret"`
}

// SecretsManagerAPI defines the interface for AWS Secrets Manager operations
type SecretsManagerAPI interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	ListSecrets(ctx context.Context, params *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error)
}

type smSecretStore struct {
	logger                     logger.Logger
	multipleKeyValuesPerSecret bool

	secretsManagerClient SecretsManagerAPI
}

// Init creates an AWS secret manager client.
func (s *smSecretStore) Init(ctx context.Context, metadata secretstores.Metadata) error {
	meta, err := s.getSecretManagerMetadata(metadata)
	if err != nil {
		return err
	}

	configOpts := awsCommonAuth.Options{
		Logger: s.logger,

		Properties: metadata.Properties,

		Region:       meta.Region,
		Endpoint:     meta.Endpoint,
		AccessKey:    meta.AccessKey, // TODO: Remove fields which should be gleaned from metadata.properties
		SecretKey:    meta.SecretKey,
		SessionToken: meta.SessionToken,
	}

	awsConfig, err := awsCommon.NewConfig(ctx, configOpts)
	if err != nil {
		return err
	}

	s.secretsManagerClient = secretsmanager.NewFromConfig(awsConfig)
	s.multipleKeyValuesPerSecret = meta.MultipleKeyValuesPerSecret

	return nil
}

func convertMapAnyToString(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		switch v := v.(type) {
		case string:
			result[k] = v
		default:
			jVal, _ := json.Marshal(v)
			result[k] = string(jVal)
		}
	}
	return result
}

func (s *smSecretStore) formatSecret(output *secretsmanager.GetSecretValueOutput) map[string]string {
	result := map[string]string{}

	if output.Name != nil && output.SecretString != nil {
		if s.multipleKeyValuesPerSecret {
			data := map[string]any{}
			if err := json.Unmarshal([]byte(*output.SecretString), &data); err != nil {
				result[*output.Name] = *output.SecretString
			} else {
				// In case of a nested JSON value, we need to stringify it
				result = convertMapAnyToString(data)
			}
		} else {
			result[*output.Name] = *output.SecretString
		}
	}

	return result
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *smSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	var versionID *string
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = &value
	}
	var versionStage *string
	if value, ok := req.Metadata[VersionStage]; ok {
		versionStage = &value
	}
	output, err := s.secretsManagerClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:     &req.Name,
		VersionId:    versionID,
		VersionStage: versionStage,
	})
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", err)
	}

	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	resp.Data = s.formatSecret(output)

	return resp, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *smSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	search := true
	var nextToken *string = nil

	for search {
		output, err := s.secretsManagerClient.ListSecrets(ctx, &secretsmanager.ListSecretsInput{
			MaxResults: nil,
			NextToken:  nextToken,
		})
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't list secrets: %s", err)
		}

		for _, entry := range output.SecretList {
			secrets, err := s.secretsManagerClient.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
				SecretId: entry.Name,
			})
			if err != nil {
				return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", *entry.Name)
			}

			resp.Data[*entry.Name] = s.formatSecret(secrets)
		}

		nextToken = output.NextToken
		search = output.NextToken != nil
	}

	return resp, nil
}

func (s *smSecretStore) getSecretManagerMetadata(spec secretstores.Metadata) (*SecretManagerMetaData, error) {
	var meta SecretManagerMetaData
	err := kitmd.DecodeMetadata(spec.Properties, &meta)
	return &meta, err
}

// Features returns the features available in this secret store.
func (s *smSecretStore) Features() []secretstores.Feature {
	if s.multipleKeyValuesPerSecret {
		return []secretstores.Feature{secretstores.FeatureMultipleKeyValuesPerSecret}
	}

	return []secretstores.Feature{}
}

func (s *smSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := SecretManagerMetaData{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (s *smSecretStore) Close() error {
	return nil
}
