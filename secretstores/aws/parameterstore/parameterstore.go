// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package parameterstore

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"

	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

// Constant literals.
const (
	VersionID = "version_id"
)

// NewParameterStore returns a new ssm parameter store.
func NewParameterStore(logger logger.Logger) secretstores.SecretStore {
	return &ssmSecretStore{logger: logger}
}

type parameterStoreMetaData struct {
	Region       string `json:"region"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

type ssmSecretStore struct {
	client ssmiface.SSMAPI
	logger logger.Logger
}

// Init creates a AWS secret manager client.
func (s *ssmSecretStore) Init(metadata secretstores.Metadata) error {
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

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *ssmSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	name := req.Name

	var versionID string
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = value
		name = fmt.Sprintf("%s:%s", req.Name, versionID)
	}

	output, err := s.client.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String(name),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", err)
	}

	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	if output.Parameter.Name != nil && output.Parameter.Value != nil {
		resp.Data[*output.Parameter.Name] = *output.Parameter.Value
	}

	return resp, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *ssmSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	search := true
	var nextToken *string = nil

	for search {
		output, err := s.client.DescribeParameters(&ssm.DescribeParametersInput{
			MaxResults: nil,
			NextToken:  nextToken,
		})
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't list secrets: %s", err)
		}

		for _, entry := range output.Parameters {
			params, err := s.client.GetParameter(&ssm.GetParameterInput{
				Name:           entry.Name,
				WithDecryption: aws.Bool(true),
			})
			if err != nil {
				return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", *entry.Name)
			}

			if entry.Name != nil && params.Parameter.Value != nil {
				resp.Data[*entry.Name] = map[string]string{*entry.Name: *params.Parameter.Value}
			}
		}

		nextToken = output.NextToken
		search = output.NextToken != nil
	}

	return resp, nil
}

func (s *ssmSecretStore) getClient(metadata *parameterStoreMetaData) (*ssm.SSM, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, "")
	if err != nil {
		return nil, err
	}

	return ssm.New(sess), nil
}

func (s *ssmSecretStore) getSecretManagerMetadata(spec secretstores.Metadata) (*parameterStoreMetaData, error) {
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
