/*
Copyright 2026 The Dapr Authors
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

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
)

type ConfigUpdater struct {
	kubeClient    kubernetes.Interface
	namespace     string
	configMapName string
	logger        logger.Logger
}

func NewKubernetesConfigUpdater(logger logger.Logger) configupdater.Updater {
	return &ConfigUpdater{
		logger: logger,
	}
}

func (u *ConfigUpdater) Init(props map[string]string) error {
	u.configMapName = props["configMapName"]
	if u.configMapName == "" {
		return errors.New("configMapName is required")
	}

	u.namespace = props["namespace"]
	if u.namespace == "" {
		if ns := os.Getenv("NAMESPACE"); ns != "" {
			u.namespace = ns
		} else {
			u.namespace = "default"
		}
	}

	kubeconfigPath := props["kubeconfigPath"]
	if kubeconfigPath == "" {
		kubeconfigPath = kubeclient.GetKubeconfigPath(u.logger, os.Args)
	}

	client, err := kubeclient.GetKubeClient(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}
	u.kubeClient = client

	// Create ConfigMap if it doesn't exist
	ctx := context.Background()
	_, err = u.kubeClient.CoreV1().ConfigMaps(u.namespace).Get(ctx, u.configMapName, metav1.GetOptions{})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("failed to check ConfigMap: %w", err)
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      u.configMapName,
				Namespace: u.namespace,
			},
			Data: map[string]string{},
		}
		_, err = u.kubeClient.CoreV1().ConfigMaps(u.namespace).Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create ConfigMap: %w", err)
		}
	}

	return nil
}

func (u *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	return u.updateConfigMapData(func(data map[string]string) {
		for key, item := range items {
			data[key] = item.Value
		}
	})
}

func (u *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	return u.AddKey(items)
}

func (u *ConfigUpdater) DeleteKey(keys []string) error {
	return u.updateConfigMapData(func(data map[string]string) {
		for _, key := range keys {
			delete(data, key)
		}
	})
}

// updateConfigMapData performs a read-modify-write on the ConfigMap's data field.
// No conflict retry is implemented because conformance tests run operations sequentially.
func (u *ConfigUpdater) updateConfigMapData(mutate func(data map[string]string)) error {
	ctx := context.Background()

	cm, err := u.kubeClient.CoreV1().ConfigMaps(u.namespace).Get(ctx, u.configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get ConfigMap: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	mutate(cm.Data)

	_, err = u.kubeClient.CoreV1().ConfigMaps(u.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update ConfigMap: %w", err)
	}

	return nil
}
