// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cloudkms

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	cloudkms "cloud.google.com/go/kms/apiv1"
	"github.com/dapr/components-contrib/secretstores"
	"google.golang.org/api/option"
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
)

// cloudkmsSecretStore is a secret store implementation of GCS KMS
type cloudkmsSecretStore struct {
	client   *cloudkms.KeyManagementClient
	metadata *cloudkmsMetadata
}

type cloudkmsMetadata struct {
	Type                string `json:"type"`
	ProjectID           string `json:"project_id"`
	PrivateKeyID        string `json:"private_key_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	ClientID            string `json:"client_id"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
	SecretFilePath      string `json:"secret_file_path"`
	KeyRingID           string `json:"key_ring_id"`
	CryptoKeyID         string `json:"crypto_key_id"`
}

// NewCloudKMSSecretStore returns a new cloudkmsSecretStore instance
func NewCloudKMSSecretStore() *cloudkmsSecretStore {
	return &cloudkmsSecretStore{}
}

// Init creates a cloudkmsClient
func (c *cloudkmsSecretStore) Init(metadata secretstores.Metadata) error {
	b, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}

	var cloudkmsMeta cloudkmsMetadata
	err = json.Unmarshal(b, &cloudkmsMeta)
	if err != nil {
		return err
	}

	clientOptions := option.WithCredentialsJSON(b)
	ctx := context.Background()
	cloudkmsClient, err := cloudkms.NewKeyManagementClient(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("error creating cloudkms client: %s", err)
	}

	c.client = cloudkmsClient
	c.metadata = &cloudkmsMeta
	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string
func (c *cloudkmsSecretStore) GetSecret(req secretstores.SecretStore) (secretstores.GetSecretResponse, error) {
	ciphertext, err := ioutil.ReadFile(c.metadata.SecretFilePath)
	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, fmt.Errorf("error reading secret file: %s", err)
	}

	name := fmt.Sprintf("projects/%s/locations/global/keyRings/%s/cryptoKeys/%s",
		c.metadata.ProjectID, c.metadata.KeyRingID, c.metadata.CryptoKeyID)
	secretResp, err := c.decryptSymmetric(name, ciphertext)

	secretValue := string(secretResp)

	if err != nil {
		return secretstores.GetSecretResponse{Data: nil}, fmt.Errorf("error occurred while decrypting: %s", err)
	}

	return secretstores.GetSecretResponse{
		Data: map[string]string{
			secretstores.DefaultSecretRefKeyName: secretValue,
		},
	}, nil
}

func (c *cloudkmsSecretStore) decryptSymmetric(name string, ciphertext []byte) ([]byte, error) {

	ctx := context.Background()
	// Build the request
	req := &kmspb.DecryptRequest{
		Name:       name,
		Ciphertext: ciphertext,
	}

	resp, err := c.client.Decrypt(ctx, req)

	if err != nil {
		return nil, fmt.Errorf("Decrypt: %v", err)
	}

	return resp.Plaintext, nil
}

func (c *cloudkmsSecretStore) parseMetadata(metadata secretstores.Metadata) ([]byte, error) {
	b, err := json.Marshal(metadata.Properties)
	return b, err
}
