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
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

var _ secretstores.SecretStore = (*kubernetesSecretStore)(nil)

type kubernetesSecretStore struct {
	kubeClient kubernetes.Interface
	md         kubernetesMetadata
	logger     logger.Logger
}

// NewKubernetesSecretStore returns a new Kubernetes secret store.
func NewKubernetesSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &kubernetesSecretStore{logger: logger}
}

// Init creates a Kubernetes client.
func (k *kubernetesSecretStore) Init(_ context.Context, metadata secretstores.Metadata) error {
	// Init metadata
	err := k.md.InitWithMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Init Kubernetes client
	kubeconfigPath := k.md.KubeconfigPath
	if kubeconfigPath == "" {
		kubeconfigPath = kubeclient.GetKubeconfigPath(k.logger, os.Args)
	}
	k.kubeClient, err = kubeclient.GetKubeClient(kubeconfigPath)
	if err != nil {
		return err
	}

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
func (k *kubernetesSecretStore) GetSecret(ctx context.Context, req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	resp := secretstores.GetSecretResponse{
		Data: map[string]string{},
	}
	namespace, err := k.getNamespaceFromMetadata(req.Metadata)
	if err != nil {
		return resp, err
	}

	secret, err := k.kubeClient.CoreV1().Secrets(namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return resp, err
	}

	for k, v := range secret.Data {
		resp.Data[k] = string(v)
	}

	return resp, nil
}

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
func (k *kubernetesSecretStore) BulkGetSecret(ctx context.Context, req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	resp := secretstores.BulkGetSecretResponse{
		Data: map[string]map[string]string{},
	}
	namespace, err := k.getNamespaceFromMetadata(req.Metadata)
	if err != nil {
		return resp, err
	}

	secrets, err := k.kubeClient.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{})
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

	if val := os.Getenv("NAMESPACE"); val != "" {
		return val, nil
	}

	if k.md.DefaultNamespace != "" {
		return k.md.DefaultNamespace, nil
	}

	return "", errors.New("namespace is missing on metadata and NAMESPACE env variable, and no default namespace is set")
}

// Features returns the features available in this secret store.
func (k *kubernetesSecretStore) Features() []secretstores.Feature {
	return []secretstores.Feature{}
}

func (k *kubernetesSecretStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	// No component metadata
	return
}

func (k *kubernetesSecretStore) Close() error {
	return nil
}
