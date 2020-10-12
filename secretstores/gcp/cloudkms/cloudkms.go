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
	"cloud.google.com/go/storage"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
	"google.golang.org/api/option"
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
)

// cloudkmsSecretStore is a secret store implementation of GCS KMS
type cloudkmsSecretStore struct {
	cloudkmsclient *cloudkms.KeyManagementClient
	storageclient  *storage.Client
	metadata       *cloudkmsMetadata
	logger         logger.Logger
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
	GCPStorageBucket    string `json:"gcp_storage_bucket"`
	SecretObject        string `json:"secret_object"`
	KeyRingID           string `json:"key_ring_id"`
	CryptoKeyID         string `json:"crypto_key_id"`
}

// NewCloudKMSSecretStore returns a new cloudkmsSecretStore instance
func NewCloudKMSSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &cloudkmsSecretStore{logger: logger}
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

	storageClient, err := storage.NewClient(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("error creating cloud storage client: %s", err)
	}

	c.cloudkmsclient = cloudkmsClient
	c.storageclient = storageClient
	c.metadata = &cloudkmsMeta

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string
func (c *cloudkmsSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	gcpStorageBucket := c.metadata.GCPStorageBucket
	secretObject := c.metadata.SecretObject

	ciphertext, err := c.getCipherTextFromSecretObject(gcpStorageBucket, secretObject)
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

func (c *cloudkmsSecretStore) getCipherTextFromSecretObject(gcpStorageBucket string, secretObject string) ([]byte, error) {
	ctx := context.Background()
	client := c.storageclient

	rc, err := client.Bucket(gcpStorageBucket).Object(secretObject).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("creation of read client failed: %s", err)
	}

	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("failed while reading secret file: %s", err)
	}

	return data, nil
}

func (c *cloudkmsSecretStore) decryptSymmetric(name string, ciphertext []byte) ([]byte, error) {
	ctx := context.Background()
	// Build the request
	req := &kmspb.DecryptRequest{
		Name:       name,
		Ciphertext: ciphertext,
	}

	resp, err := c.cloudkmsclient.Decrypt(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %v", err)
	}

	return resp.Plaintext, nil
}

func (c *cloudkmsSecretStore) parseMetadata(metadata secretstores.Metadata) ([]byte, error) {
	b, err := json.Marshal(metadata.Properties)

	return b, err
}
