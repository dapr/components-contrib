// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package secretmanager

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/secretsmanager/secretsmanageriface"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	VersionID    = "version_id"
	VersionStage = "version_stage"
)

// NewSecretManager returns a new secret manager store
func NewSecretManager(logger logger.Logger) secretstores.SecretStore {
	return &smSecretStore{logger: logger}
}

type secretManagerMetaData struct {
	Region       string `json:"region"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

type smSecretStore struct {
	client secretsmanageriface.SecretsManagerAPI
	logger logger.Logger
}

// Init creates a AWS secret manager client
func (s *smSecretStore) Init(metadata secretstores.Metadata) error {
	meta, err := s.getSecretManagerMetadata(metadata)
	if err != nil {
		return err
	}

	client, err := s.getClient(meta)
	if err != nil {
		return err
	}
	s.client = client

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
func (s *smSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	var versionID *string
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = &value
	}
	var versionStage *string
	if value, ok := req.Metadata[VersionStage]; ok {
		versionStage = &value
	}

	output, err := s.client.GetSecretValue(&secretsmanager.GetSecretValueInput{
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

func (s *smSecretStore) getClient(metadata *secretManagerMetaData) (*secretsmanager.SecretsManager, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.Region, "")
	if err != nil {
		return nil, err
	}

	return secretsmanager.New(sess), nil
}

func (s *smSecretStore) getSecretManagerMetadata(spec secretstores.Metadata) (*secretManagerMetaData, error) {
	b, err := json.Marshal(spec.Properties)
	if err != nil {
		return nil, err
	}

	var meta secretManagerMetaData
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}
