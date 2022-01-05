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

package kubernetes

import (
	"context"
	"errors"
	"os"

	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeclient "github.com/dapr/components-contrib/authentication/kubernetes"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

type kubernetesSecretStore struct {
	kubeClient kubernetes.Interface
	logger     logger.Logger
}

// NewKubernetesSecretStore returns a new Kubernetes secret store.
func NewKubernetesSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &kubernetesSecretStore{logger: logger}
}

// Init creates a Kubernetes client.
func (k *kubernetesSecretStore) Init(metadata secretstores.Metadata) error {
	client, err := kubeclient.GetKubeClient()
	if err != nil {
		return err
	}
	k.kubeClient = client

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (k *kubernetesSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	namespace, err := k.getNamespaceFromMetadata(req.Metadata)
	if err != nil {
		return resp, err
	}

	secret, err := k.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), req.Name, meta_v1.GetOptions{})
	if err != nil {
		return resp, err
	}

	for k, v := range secret.Data {
		resp.Data[k] = string(v)
	}

	return resp, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (k *kubernetesSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}
	namespace, err := k.getNamespaceFromMetadata(req.Metadata)
	if err != nil {
		return resp, err
	}

	secrets, err := k.kubeClient.CoreV1().Secrets(namespace).List(context.TODO(), meta_v1.ListOptions{})
	if err != nil {
		return resp, err
	}

	for _, s := range secrets.Items {
		resp.Data[s.Name] = map[string]string{}
		for k, v := range s.Data {
			resp.Data[s.Name][k] = string(v)
		}
	}

	return resp, nil
}

func (k *kubernetesSecretStore) getNamespaceFromMetadata(metadata map[string]string) (string, error) {
	if val, ok := metadata["namespace"]; ok && val != "" {
		return val, nil
	}

	val := os.Getenv("NAMESPACE")
	if val != "" {
		return val, nil
	}

	return "", errors.New("namespace is missing on metadata and NAMESPACE env variable")
}
