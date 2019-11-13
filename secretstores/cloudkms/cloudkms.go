// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cloudkms

import (
	"context"
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
	client   *cloudkms.KeyManagementClient
	metadata *cloudkmsMetadata
}

type cloudkmsMetadata struct {
}

func NewCloudKMSSecretStore() *cloudkmsSecretStore {
	return &cloudkmsSecretStore{}
}

// Init creates a cloudkmsClient
func (v *cloudkmsSecretStore) Init(metadata secretstores.Metadata) error {
	//props := metadata.Properties
	//projectId := props[projectId]
	//locationId := props[locationId]

	ctx := context.Background()
	cloudkmsClient, err := cloudkms.NewKeyManagementClient(ctx)

	if err != nil {
		return fmt.Errorf("error creating cloudkms client: %s", err)
	}

	v.client = cloudkmsClient
	return nil
}
