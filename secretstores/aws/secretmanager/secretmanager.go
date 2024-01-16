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
	"errors"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/secretsmanager/secretsmanageriface"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/common/utils"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
	Region       string `json:"region"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

type smSecretStore struct {
	client secretsmanageriface.SecretsManagerAPI
	logger logger.Logger
}

// Init creates an AWS secret manager client.
func (s *smSecretStore) Init(ctx context.Context, metadata secretstores.Metadata) error {
	meta, err := s.getSecretManagerMetadata(metadata)
	if err != nil {
		return err
	}

	// This check is needed because d.client is set to a mock in tests
	if s.client == nil {
		s.client, err = s.getClient(meta)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}

	var notFoundErr *secretsmanager.ResourceNotFoundException
	if err := s.validateConnection(ctx); err != nil && !errors.As(err, &notFoundErr) {
		return fmt.Errorf("error validating access to the aws.secretmanager secret store: %w", err)
	}
	return nil
}

// validateConnection runs a dummy GetSecretValueWithContext operation
// to validate the connection credentials
func (s *smSecretStore) validateConnection(ctx context.Context) error {
	_, err := s.client.GetSecretValueWithContext(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: ptr.Of(utils.GetRandOrDefaultString("dapr-test-secret")),
	})

	return err
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

	output, err := s.client.GetSecretValueWithContext(ctx, &secretsmanager.GetSecretValueInput{
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
	if output.Name != nil && output.SecretString != nil {
		resp.Data[*output.Name] = *output.SecretString
	}

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
		output, err := s.client.ListSecretsWithContext(ctx, &secretsmanager.ListSecretsInput{
			MaxResults: nil,
			NextToken:  nextToken,
		})
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't list secrets: %s", err)
		}

		for _, entry := range output.SecretList {
			secrets, err := s.client.GetSecretValueWithContext(ctx, &secretsmanager.GetSecretValueInput{
				SecretId: entry.Name,
			})
			if err != nil {
				return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", *entry.Name)
			}

			if entry.Name != nil && secrets.SecretString != nil {
				resp.Data[*entry.Name] = map[string]string{*entry.Name: *secrets.SecretString}
			}
		}

		nextToken = output.NextToken
		search = output.NextToken != nil
	}

	return resp, nil
}

func (s *smSecretStore) getClient(metadata *SecretManagerMetaData) (*secretsmanager.SecretsManager, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, "")
	if err != nil {
		return nil, err
	}

	return secretsmanager.New(sess), nil
}

func (s *smSecretStore) getSecretManagerMetadata(spec secretstores.Metadata) (*SecretManagerMetaData, error) {
	b, err := json.Marshal(spec.Properties)
	if err != nil {
		return nil, err
	}

	var meta SecretManagerMetaData
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

// Features returns the features available in this secret store.
func (s *smSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (s *smSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := SecretManagerMetaData{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}
