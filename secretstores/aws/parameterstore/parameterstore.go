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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

// Constant literals.
const (
	VersionID = "version_id"
)

var _ secretstores.SecretStore = (*ssmSecretStore)(nil)

// NewParameterStore returns a new ssm parameter store.
func NewParameterStore(logger logger.Logger) secretstores.SecretStore {
	return &ssmSecretStore{logger: logger}
}

type ParameterStoreMetaData struct {
	// Ignored by metadata parser because included in built-in authentication profile
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`

	Region string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	Prefix string `json:"prefix"`
}

type ssmSecretStore struct {
	client ssmiface.SSMAPI
	prefix string
	logger logger.Logger
}

// Init creates an AWS secret manager client.
func (s *ssmSecretStore) Init(ctx context.Context, metadata secretstores.Metadata) error {
	meta, err := s.getSecretManagerMetadata(metadata)
	if err != nil {
		return err
	}

	s.client, err = s.getClient(meta)
	if err != nil {
		return err
	}
	s.prefix = meta.Prefix

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *ssmSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	name := req.Name

	var versionID string
	if value, ok := req.Metadata[VersionID]; ok {
		versionID = value
		name = fmt.Sprintf("%s:%s", req.Name, versionID)
	}

	output, err := s.client.GetParameterWithContext(ctx, &ssm.GetParameterInput{
		Name:           ptr.Of(s.prefix + name),
		WithDecryption: ptr.Of(true),
	})
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", err)
	}

	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	if output.Parameter.Name != nil && output.Parameter.Value != nil {
		secretName := (*output.Parameter.Name)[len(s.prefix):]
		resp.Data[secretName] = *output.Parameter.Value
	}

	return resp, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (s *ssmSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	search := true
	var nextToken *string = nil

	var filters []*ssm.ParameterStringFilter
	if s.prefix != "" {
		filters = []*ssm.ParameterStringFilter{
			{
				Key:    aws.String(ssm.ParametersFilterKeyName),
				Option: aws.String("BeginsWith"),
				Values: aws.StringSlice([]string{s.prefix}),
			},
		}
	}

	for search {
		output, err := s.client.DescribeParametersWithContext(ctx, &ssm.DescribeParametersInput{
			MaxResults:       nil,
			NextToken:        nextToken,
			ParameterFilters: filters,
		})
		if err != nil {
			return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't list secrets: %s", err)
		}

		for _, entry := range output.Parameters {
			params, err := s.client.GetParameterWithContext(ctx, &ssm.GetParameterInput{
				Name:           entry.Name,
				WithDecryption: aws.Bool(true),
			})
			if err != nil {
				return secretstores.BulkGetSecretResponse{Data: nil}, fmt.Errorf("couldn't get secret: %s", *entry.Name)
			}

			if entry.Name != nil && params.Parameter.Value != nil {
				secretName := (*entry.Name)[len(s.prefix):]
				resp.Data[secretName] = map[string]string{secretName: *params.Parameter.Value}
			}
		}

		nextToken = output.NextToken
		search = output.NextToken != nil
	}

	return resp, nil
}

func (s *ssmSecretStore) getClient(metadata *ParameterStoreMetaData) (*ssm.SSM, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, "")
	if err != nil {
		return nil, err
	}

	return ssm.New(sess), nil
}

func (s *ssmSecretStore) getSecretManagerMetadata(spec secretstores.Metadata) (*ParameterStoreMetaData, error) {
	meta := ParameterStoreMetaData{}
	err := kitmd.DecodeMetadata(spec.Properties, &meta)
	return &meta, err
}

// Features returns the features available in this secret store.
func (s *ssmSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (s *ssmSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := ParameterStoreMetaData{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (s *ssmSecretStore) Close() error {
	return nil
}
