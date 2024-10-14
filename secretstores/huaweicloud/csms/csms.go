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

package csms

import (
	"context"
	"fmt"
	"reflect"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/basic"
	csms "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/csms/v1"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/csms/v1/model"
	csmsRegion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/csms/v1/region"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	pageLimit     string = "100"
	latestVersion string = "latest"
	versionID     string = "version_id"
)

type csmsClient interface {
	ListSecrets(request *model.ListSecretsRequest) (*model.ListSecretsResponse, error)
	ShowSecretVersion(request *model.ShowSecretVersionRequest) (*model.ShowSecretVersionResponse, error)
}

var _ secretstores.SecretStore = (*csmsSecretStore)(nil)

type csmsSecretStore struct {
	client csmsClient
	logger logger.Logger
}

type CsmsSecretStoreMetadata struct {
	Region          string
	AccessKey       string
	SecretAccessKey string
}

// NewHuaweiCsmsSecretStore returns a new Huawei csms secret store.
func NewHuaweiCsmsSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &csmsSecretStore{logger: logger}
}

// Init creates a Huawei csms client.
func (c *csmsSecretStore) Init(ctx context.Context, meta secretstores.Metadata) error {
	m := CsmsSecretStoreMetadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}
	auth := basic.NewCredentialsBuilder().
		WithAk(m.AccessKey).
		WithSk(m.SecretAccessKey).
		Build()

	c.client = csms.NewCsmsClient(
		csms.CsmsClientBuilder().
			WithRegion(csmsRegion.ValueOf(m.Region)).
			WithCredential(auth).
			Build())

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (c *csmsSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	request := &model.ShowSecretVersionRequest{}
	request.SecretName = req.Name
	if value, ok := req.Metadata[versionID]; ok {
		request.VersionId = value
	}

	response, err := c.client.ShowSecretVersion(request)
	if err != nil {
		return secretstores.GetSecretResponse{}, err
	}

	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: *response.Version.SecretString,
		},
	}, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (c *csmsSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	secretNames, err := c.getSecretNames(ctx, nil)
	if err != nil {
		return secretstores.BulkGetSecretResponse{}, err
	}

	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}

	for _, secretName := range secretNames {
		secret, err := c.GetSecret(ctx, secretstores.GetSecretRequest{
			Name: secretName,
			Metadata: map[string]string{
				versionID: latestVersion,
			},
		})
		if err != nil {
			return secretstores.BulkGetSecretResponse{}, err
		}

		resp.Data[secretName] = secret.Data
	}

	return resp, nil
}

// Get all secret names recursively.
func (c *csmsSecretStore) getSecretNames(ctx context.Context, marker *string) ([]string, error) {
	request := &model.ListSecretsRequest{}
	limit := pageLimit
	request.Limit = &limit
	request.Marker = marker

	response, err := c.client.ListSecrets(request)
	if err != nil {
		return nil, err
	}

	resp := make([]string, 0, len(*response.Secrets))
	for _, secret := range *response.Secrets {
		resp = append(resp, *secret.Name)
	}

	// If the NextMarker has value then continue to retrieve data from next page.
	if response.PageInfo.NextMarker != nil {
		nextResp, err := c.getSecretNames(ctx, response.PageInfo.NextMarker)
		if err != nil {
			return nil, err
		}

		resp = append(resp, nextResp...)
	}

	return resp, nil
}

// Features returns the features available in this secret store.
func (c *csmsSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{} // No Feature supported.
}

func (c *csmsSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := CsmsSecretStoreMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.SecretStoreType)
	return
}

func (c *csmsSecretStore) Close() error {
	return nil
}
