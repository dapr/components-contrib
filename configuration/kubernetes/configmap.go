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
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

var _ configuration.Store = (*ConfigurationStore)(nil)

// subscriber holds the state for a single subscription registered via Subscribe.
type subscriber struct {
	req     *configuration.SubscribeRequest
	handler configuration.UpdateHandler
	ctx     context.Context
	cancel  context.CancelFunc
}

// ConfigurationStore implements a Kubernetes ConfigMap-backed configuration store.
// A single shared informer watches the configured ConfigMap and fans out change
// events to all registered subscribers, avoiding per-subscription watches against
// the API server.
type ConfigurationStore struct {
	kubeClient kubernetes.Interface
	metadata   metadata
	logger     logger.Logger

	// subscribers maps subscription ID → *subscriber for fan-out.
	subscribers sync.Map

	// Shared informer lifecycle: started once on first Subscribe, stopped on Close.
	informerOnce   sync.Once
	informerCancel context.CancelFunc
	informerWg     sync.WaitGroup

	lock   sync.RWMutex
	closed atomic.Bool
}

// NewKubernetesConfigMapStore returns a new Kubernetes ConfigMap configuration store.
func NewKubernetesConfigMapStore(logger logger.Logger) configuration.Store {
	return &ConfigurationStore{
		logger: logger,
	}
}

func (s *ConfigurationStore) Init(ctx context.Context, meta configuration.Metadata) error {
	if err := s.metadata.parse(meta); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	if s.kubeClient == nil {
		kubeconfigPath := s.metadata.KubeconfigPath
		if kubeconfigPath == "" {
			kubeconfigPath = kubeclient.GetKubeconfigPath(s.logger, os.Args)
		}

		client, err := kubeclient.GetKubeClient(kubeconfigPath)
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client: %w", err)
		}
		s.kubeClient = client
	}

	ns := s.resolveNamespace(nil)
	_, err := s.kubeClient.CoreV1().ConfigMaps(ns).Get(ctx, s.metadata.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get ConfigMap %q in namespace %q: %w", s.metadata.ConfigMapName, ns, err)
	}

	return nil
}

func (s *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	ns := s.resolveNamespace(req.Metadata)
	cm, err := s.kubeClient.CoreV1().ConfigMaps(ns).Get(ctx, s.metadata.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get ConfigMap %q: %w", s.metadata.ConfigMapName, err)
	}

	items := make(map[string]*configuration.Item)

	if len(req.Keys) == 0 {
		for k, v := range cm.Data {
			items[k] = &configuration.Item{
				Value:    v,
				Version:  cm.ResourceVersion,
				Metadata: map[string]string{},
			}
		}
		for k, v := range cm.BinaryData {
			items[k] = &configuration.Item{
				Value:    base64.StdEncoding.EncodeToString(v),
				Version:  cm.ResourceVersion,
				Metadata: map[string]string{"encoding": "base64"},
			}
		}
	} else {
		for _, key := range req.Keys {
			if v, ok := cm.Data[key]; ok {
				items[key] = &configuration.Item{
					Value:    v,
					Version:  cm.ResourceVersion,
					Metadata: map[string]string{},
				}
			} else if v, ok := cm.BinaryData[key]; ok {
				items[key] = &configuration.Item{
					Value:    base64.StdEncoding.EncodeToString(v),
					Version:  cm.ResourceVersion,
					Metadata: map[string]string{"encoding": "base64"},
				}
			}
		}
	}

	return &configuration.GetResponse{Items: items}, nil
}

func (s *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.closed.Load() {
		return "", errors.New("configuration store is closed")
	}

	subscribeID := uuid.New().String()
	childCtx, cancel := context.WithCancel(ctx)

	sub := &subscriber{
		req:     req,
		handler: handler,
		ctx:     childCtx,
		cancel:  cancel,
	}
	s.subscribers.Store(subscribeID, sub)

	// Start the shared informer on first subscription.
	s.startInformer()

	return subscribeID, nil
}

// startInformer lazily starts a single informer that watches the configured
// ConfigMap and fans out change events to all registered subscribers.
func (s *ConfigurationStore) startInformer() {
	s.informerOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.informerCancel = cancel

		ns := s.resolveNamespace(nil)

		watchlist := cache.NewFilteredListWatchFromClient(
			s.kubeClient.CoreV1().RESTClient(),
			"configmaps",
			ns,
			func(options *metav1.ListOptions) {
				options.FieldSelector = fields.OneTermEqualSelector("metadata.name", s.metadata.ConfigMapName).String()
			},
		)

		// Only UpdateFunc is registered: subscribers receive change-only notifications.
		// AddFunc is intentionally omitted — the initial List populates the informer
		// cache but does not notify, matching the behavior of the Redis and PostgreSQL
		// configuration stores. DeleteFunc is omitted because ConfigMap-level deletion
		// is not a supported use case; only key-level changes within the ConfigMap
		// are tracked.
		_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
			ListerWatcher: watchlist,
			ObjectType:    &corev1.ConfigMap{},
			ResyncPeriod:  s.metadata.ResyncPeriod,
			Handler: cache.ResourceEventHandlerFuncs{
				UpdateFunc: func(oldObj, newObj any) {
					s.fanOutUpdate(oldObj, newObj)
				},
			},
		})

		s.informerWg.Add(1)
		go func() {
			defer s.informerWg.Done()
			controller.Run(ctx.Done())
		}()
	})
}

// fanOutUpdate dispatches ConfigMap change events to all registered subscribers.
func (s *ConfigurationStore) fanOutUpdate(oldObj, newObj any) {
	oldCM, ok1 := oldObj.(*corev1.ConfigMap)
	newCM, ok2 := newObj.(*corev1.ConfigMap)
	if !ok1 || !ok2 {
		s.logger.Warn("received non-ConfigMap object in update handler")
		return
	}

	s.subscribers.Range(func(key, value any) bool {
		sub := value.(*subscriber)
		subscriptionID := key.(string)

		// Skip and clean up subscribers whose context is done.
		if sub.ctx.Err() != nil {
			s.subscribers.Delete(key)
			return true
		}

		changedItems := computeChangedItems(sub.req.Keys, oldCM, newCM)
		if len(changedItems) == 0 {
			return true
		}

		if err := sub.handler(sub.ctx, &configuration.UpdateEvent{
			ID:    subscriptionID,
			Items: changedItems,
		}); err != nil {
			s.logger.Errorf("failed to notify handler for subscription %s: %v", subscriptionID, err)
		}

		return true
	})
}

// computeChangedItems computes the set of configuration items that changed
// between oldCM and newCM, filtered to only the given subscribedKeys (or all
// keys if subscribedKeys is empty).
func computeChangedItems(subscribedKeys []string, oldCM, newCM *corev1.ConfigMap) map[string]*configuration.Item {
	changedItems := make(map[string]*configuration.Item)

	// Detect added or modified keys in data.
	for k, newVal := range newCM.Data {
		oldVal, existed := oldCM.Data[k]
		if !existed || oldVal != newVal {
			if isSubscribedKey(subscribedKeys, k) {
				changedItems[k] = &configuration.Item{
					Value:    newVal,
					Version:  newCM.ResourceVersion,
					Metadata: map[string]string{},
				}
			}
		}
	}

	// Detect added or modified keys in binaryData.
	for k, newVal := range newCM.BinaryData {
		oldVal, existed := oldCM.BinaryData[k]
		if !existed || !bytes.Equal(oldVal, newVal) {
			if isSubscribedKey(subscribedKeys, k) {
				changedItems[k] = &configuration.Item{
					Value:    base64.StdEncoding.EncodeToString(newVal),
					Version:  newCM.ResourceVersion,
					Metadata: map[string]string{"encoding": "base64"},
				}
			}
		}
	}

	// Detect deleted keys: a key is deleted only if it existed in old data or
	// binaryData and is now absent from BOTH new data and binaryData. This
	// prevents a false deletion when a key moves between data and binaryData.
	allOldKeys := make(map[string]struct{})
	for k := range oldCM.Data {
		allOldKeys[k] = struct{}{}
	}
	for k := range oldCM.BinaryData {
		allOldKeys[k] = struct{}{}
	}
	for k := range allOldKeys {
		_, inNewData := newCM.Data[k]
		_, inNewBinary := newCM.BinaryData[k]
		if !inNewData && !inNewBinary {
			if isSubscribedKey(subscribedKeys, k) {
				changedItems[k] = &configuration.Item{
					Value:    "",
					Version:  newCM.ResourceVersion,
					Metadata: map[string]string{"deleted": "true"},
				}
			}
		}
	}

	return changedItems
}

func (s *ConfigurationStore) Unsubscribe(_ context.Context, req *configuration.UnsubscribeRequest) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if sub, ok := s.subscribers.LoadAndDelete(req.ID); ok {
		sub.(*subscriber).cancel()
		return nil
	}

	return fmt.Errorf("subscription with id %s does not exist", req.ID)
}

func (s *ConfigurationStore) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.ConfigurationStoreType)
	return metadataInfo
}

func (s *ConfigurationStore) Close() error {
	// Set closed before acquiring the write lock so that new Subscribe calls
	// are rejected immediately, even while we wait for existing read-lock holders
	// (in-flight Subscribe/Unsubscribe) to finish.
	s.closed.Store(true)

	s.lock.Lock()
	defer s.lock.Unlock()

	// Cancel all subscriber contexts.
	s.subscribers.Range(func(key, value any) bool {
		value.(*subscriber).cancel()
		return true
	})
	s.subscribers.Clear()

	// Stop the shared informer if it was started.
	if s.informerCancel != nil {
		s.informerCancel()
	}
	s.informerWg.Wait()

	return nil
}

func (s *ConfigurationStore) resolveNamespace(requestMetadata map[string]string) string {
	if ns, ok := requestMetadata["namespace"]; ok && ns != "" {
		if errs := validation.IsDNS1123Label(ns); len(errs) == 0 {
			return ns
		}
		s.logger.Warnf("ignoring invalid namespace override %q in request metadata", ns)
	}
	// Use the NAMESPACE env var only when the component metadata did not
	// explicitly set a namespace value.
	if !s.metadata.namespaceExplicit {
		if ns := os.Getenv("NAMESPACE"); ns != "" {
			return ns
		}
	}
	return s.metadata.Namespace
}

func isSubscribedKey(subscribedKeys []string, key string) bool {
	if len(subscribedKeys) == 0 {
		return true
	}
	return slices.Contains(subscribedKeys, key)
}
