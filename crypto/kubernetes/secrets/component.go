/*
Copyright 2023 The Dapr Authors
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

package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	contribCrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
	internals "github.com/dapr/kit/crypto"
	"github.com/dapr/kit/logger"
)

const (
	requestTimeout              = 30 * time.Second
	metadataKeyDefaultNamespace = "defaultNamespace"
)

type kubeSecretsCrypto struct {
	contribCrypto.LocalCryptoBaseComponent

	logger     logger.Logger
	md         secretsMetadata
	kubeClient kubernetes.Interface
}

// NewKubeSecretsCrypto returns a new Kubernetes secrets crypto provider.
// The key arguments in methods can be in the format "namespace/secretName/key" or "secretName/key" if using the default namespace passed as component metadata.
func NewKubeSecretsCrypto(log logger.Logger) contribCrypto.SubtleCrypto {
	k := &kubeSecretsCrypto{
		logger: log,
	}
	k.RetrieveKeyFn = k.retrieveKeyFromSecret
	return k
}

// Init the crypto provider.
func (k *kubeSecretsCrypto) Init(_ context.Context, metadata contribCrypto.Metadata) error {
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
		return fmt.Errorf("failed to init Kubernetes client: %w", err)
	}

	return nil
}

// Features returns the features available in this crypto provider.
func (k *kubeSecretsCrypto) Features() []contribCrypto.Feature {
	return []contribCrypto.Feature{} // No Feature supported.
}

// Retrieves a key (public or private or symmetric) from a Kubernetes secret.
func (k *kubeSecretsCrypto) retrieveKeyFromSecret(parentCtx context.Context, key string) (jwk.Key, error) {
	keyNamespace, keySecret, keyName, err := k.parseKeyString(key)
	if err != nil {
		return nil, err
	}

	// Retrieve the secret
	ctx, cancel := context.WithTimeout(parentCtx, requestTimeout)
	res, err := k.kubeClient.CoreV1().
		Secrets(keyNamespace).
		Get(ctx, keySecret, metaV1.GetOptions{})
	cancel()
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Data) == 0 || len(res.Data[keyName]) == 0 {
		return nil, contribCrypto.ErrKeyNotFound
	}

	// Parse the key
	jwkObj, err := internals.ParseKey(res.Data[keyName], string(res.Type))
	if err == nil {
		switch jwkObj.KeyType() {
		case jwa.EC, jwa.RSA, jwa.OKP, jwa.OctetSeq:
			// Nop
		default:
			err = errors.New("invalid key type")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse key from secret: %w", err)
	}

	return jwkObj, nil
}

// parseKeyString returns the secret name, key, and optional namespace from the key parameter.
// If the key parameter doesn't contain a namespace, returns the default one.
func (k *kubeSecretsCrypto) parseKeyString(param string) (namespace string, secret string, key string, err error) {
	parts := strings.Split(key, "/")
	switch len(parts) {
	case 3:
		namespace = parts[0]
		secret = parts[1]
		key = parts[2]
	case 2:
		namespace = k.md.DefaultNamespace
		secret = parts[0]
		key = parts[1]
	default:
		err = errors.New("key is not in a valid format: required namespace/secretName/key or secretName/key")
	}

	if namespace == "" {
		err = errors.New("key doesn't have a namespace and the default namespace isn't set")
	}

	return
}

func (kubeSecretsCrypto) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := secretsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.CryptoType)
	return
}

func (kubeSecretsCrypto) Close() error {
	return nil
}
