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

package ssm

import (
	"context"
	"errors"
	"strconv"

	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	ssm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ssm/v20190923"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

const (
	SecretID         = "SecretID"
	SecretKey        = "SecretKey"
	Token            = "Token"
	Region           = "Region"
	VersionID        = "VersionID"
	RequestID        = "RequestID"
	ValueType        = "SecretValueType"
	pageLimit uint64 = 100
)

type SecretValueType uint16

var (
	// TextSecretValue text secret.
	TextSecretValue SecretValueType = 10
	// BinarySecretValue binary secret.
	BinarySecretValue SecretValueType = 20
)

type ssmClient interface {
	ListSecretsWithContext(ctx context.Context, request *ssm.ListSecretsRequest) (*ssm.ListSecretsResponse, error)
	GetSecretValueWithContext(ctx context.Context, request *ssm.GetSecretValueRequest) (*ssm.GetSecretValueResponse, error)
}

type ssmSecretStore struct {
	client ssmClient
	logger logger.Logger
}

// NewSSM returns a new TencentCloud ssm secret store.
func NewSSM(logger logger.Logger) secretstores.SecretStore {
	return &ssmSecretStore{
		logger: logger,
	}
}

// Init creates a TencentCloud ssm client.
func (s *ssmSecretStore) Init(metadata secretstores.Metadata) error {
	var (
		err    error
		region string
	)

	secretID := metadata.Properties[SecretID]
	secretKey := metadata.Properties[SecretKey]
	token := metadata.Properties[Token]
	region = metadata.Properties[Region]
	if secretID == "" || secretKey == "" {
		return errors.New("secret params are empty")
	}

	credential := common.NewTokenCredential(secretID, secretKey, token)
	s.client, err = ssm.NewClient(credential, region, profile.NewClientProfile())
	if err != nil {
		return err
	}

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (s *ssmSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	response := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	if req.Name == "" {
		return response, errors.New("secret name is empty")
	}

	versionID := req.Metadata[VersionID]
	ssmReq := ssm.NewGetSecretValueRequest()
	ssmReq.SecretName = &req.Name
	ssmReq.VersionId = &versionID
	ssResp, err := s.client.GetSecretValueWithContext(ctx, ssmReq)
	if err != nil {
		return response, err
	}
	var val string
	ssResponse := ssResp.Response
	if ssResponse.SecretBinary != nil {
		response.Data[ValueType] = strconv.Itoa(int(BinarySecretValue))
		val = *ssResponse.SecretBinary
	} else if ssResponse.SecretString != nil {
		response.Data[ValueType] = strconv.Itoa(int(TextSecretValue))
		val = *ssResponse.SecretString
	}

	if ssResponse.SecretName != nil {
		response.Data[*ssResponse.SecretName] = val
	}
	if ssResponse.RequestId != nil {
		response.Data[RequestID] = *ssResponse.RequestId
	}

	return response, nil
}

func (s *ssmSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	response := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}
	versionID := req.Metadata[VersionID]

	var start uint64 = 0
	names, err := s.getSecretNames(ctx, &start)
	if err != nil {
		return response, err
	}

	for _, name := range names {
		resp, err := s.GetSecret(ctx, secretstores.GetSecretRequest{
			Name: name,
			Metadata: map[string]string{
				VersionID: versionID,
			},
		})
		if err != nil {
			return response, err
		}
		response.Data[name] = resp.Data
	}

	return response, nil
}

func (s *ssmSecretStore) getSecretNames(ctx context.Context, offset *uint64) ([]string, error) {
	names := []string{}
	limit := pageLimit
	// secret name is in Enabled state.
	var state uint64 = 1
	ssmReq := ssm.NewListSecretsRequest()
	ssmReq.Offset = offset
	ssmReq.Limit = &limit
	ssmReq.State = &state
	ssmResp, err := s.client.ListSecretsWithContext(ctx, ssmReq)
	if err != nil {
		return nil, err
	}

	resp := ssmResp.Response
	for _, metadata := range resp.SecretMetadatas {
		names = append(names, *metadata.SecretName)
	}
	retLen := uint64(len(resp.SecretMetadatas))
	if limit > retLen {
		return names, nil
	}

	var newNames []string
	*offset += retLen
	newNames, err = s.getSecretNames(ctx, offset)
	if err != nil {
		return nil, err
	}
	names = append(names, newNames...)

	return names, nil
}

// Features returns the features available in this secret store.
func (s *ssmSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}
