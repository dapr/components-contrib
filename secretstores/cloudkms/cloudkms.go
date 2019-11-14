// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cloudkms

import (
	"context"
	"encoding/json"
	"fmt"

	cloudkms "cloud.google.com/go/kms/apiv1"
	"github.com/dapr/components-contrib/secretstores"
)

const (
	projectId  string = "projectId"
	locationId string = "locationId"
)

// cloudkmsSecretStore is a secret store implementation of GCS KMS
type cloudkmsSecretStore struct {
	client *cloudkms.KeyManagementClient
}

type cloudkmsMetadata struct {
}

// NewCloudKMSSecretStore returns a new cloudkmsSecretStore instance
func NewCloudKMSSecretStore() *cloudkmsSecretStore {
	return &cloudkmsSecretStore{}
}

// Init creates a cloudkmsClient
func (c *cloudkmsSecretStore) Init(metadata secretstores.Metadata) error {
	//props := metadata.Properties
	//projectId := props[projectId]
	//locationId := props[locationId]
	b, err := c.parseMetadata(metadata)

	ctx := context.Background()
	cloudkmsClient, err := cloudkms.NewKeyManagementClient(ctx)

	if err != nil {
		return fmt.Errorf("error creating cloudkms client: %s", err)
	}

	c.client = cloudkmsClient

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string
func (c *cloudkmsSecretStore) GetSecret(req secretstores.SecretStore) (secretstores.GetSecretResponse, error) {
	token, err := c.r
}

func (c *cloudkmsSecretStore) parseMetadata(metadata secretstores.Metadata) ([]byte, error) {
	b, err := json.Marshal(metadata.Properties)
	return b, err
}
