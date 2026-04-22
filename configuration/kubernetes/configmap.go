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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

var _ configuration.Store = (*ConfigurationStore)(nil)

// ConfigurationStore implements a Kubernetes ConfigMap-backed configuration store.
// A SharedIndexInformer watches the configured ConfigMap. Each Subscribe call
// registers a per-subscriber event handler on the informer, which handles
// initial state replay, change notifications, and deletion atomically.
type ConfigurationStore struct {
	kubeClient kubernetes.Interface
	metadata   metadata
	namespace  string
	logger     logger.Logger

	informer       cache.SharedIndexInformer
	informerCancel context.CancelFunc
	informerWg     sync.WaitGroup

	mu            sync.Mutex
	subscriptions map[string]*subscription
	closed        atomic.Bool
}

// subscription holds the informer registration and cancel function for a
// single Subscribe call.
type subscription struct {
	registration cache.ResourceEventHandlerRegistration
	cancel       context.CancelFunc
}

// NewKubernetesConfigMapStore returns a new Kubernetes ConfigMap configuration store.
func NewKubernetesConfigMapStore(logger logger.Logger) configuration.Store {
	return &ConfigurationStore{
		logger:        logger,
		subscriptions: make(map[string]*subscription),
	}
}

func (s *ConfigurationStore) Init(ctx context.Context, meta configuration.Metadata) error {
	if err := s.metadata.parse(meta); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	s.namespace = resolveNamespace()

	if s.kubeClient == nil {
		kubeconfigPath := ""
		if s.metadata.KubeconfigPath != nil {
			kubeconfigPath = *s.metadata.KubeconfigPath
		} else {
			kubeconfigPath = kubeclient.GetKubeconfigPath(s.logger, os.Args)
		}

		client, err := kubeclient.GetKubeClient(kubeconfigPath)
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client: %w", err)
		}
		s.kubeClient = client
	}

	if err := s.startInformer(ctx); err != nil {
		return fmt.Errorf("failed to start informer: %w", err)
	}

	return nil
}

func (s *ConfigurationStore) Get(_ context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	obj, exists, err := s.informer.GetStore().GetByKey(s.namespace + "/" + s.metadata.ConfigMapName)
	if err != nil {
		return nil, fmt.Errorf("failed to get ConfigMap %q from cache: %w", s.metadata.ConfigMapName, err)
	}
	if !exists {
		return &configuration.GetResponse{Items: map[string]*configuration.Item{}}, nil
	}

	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("unexpected object type in cache for ConfigMap %q", s.metadata.ConfigMapName)
	}

	allItems := configMapToItems(cm)
	return &configuration.GetResponse{Items: filterByKeys(allItems, req.Keys)}, nil
}

func (s *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	if s.closed.Load() {
		return "", errors.New("configuration store is closed")
	}

	subscribeID := uuid.New().String()
	childCtx, cancel := context.WithCancel(ctx)

	subHandler := &subscriberHandler{
		id:      subscribeID,
		keys:    req.Keys,
		handler: handler,
		ctx:     childCtx,
		logger:  s.logger,
	}

	reg, err := s.informer.AddEventHandler(subHandler)
	if err != nil {
		cancel()
		return "", fmt.Errorf("failed to add event handler: %w", err)
	}

	s.mu.Lock()
	s.subscriptions[subscribeID] = &subscription{
		registration: reg,
		cancel:       cancel,
	}
	s.mu.Unlock()

	return subscribeID, nil
}

func (s *ConfigurationStore) Unsubscribe(_ context.Context, req *configuration.UnsubscribeRequest) error {
	s.mu.Lock()
	sub, ok := s.subscriptions[req.ID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("subscription with id %s does not exist", req.ID)
	}
	delete(s.subscriptions, req.ID)
	s.mu.Unlock()

	if err := s.informer.RemoveEventHandler(sub.registration); err != nil {
		s.logger.Errorf("failed to remove event handler for subscription %s: %v", req.ID, err)
	}
	sub.cancel()
	return nil
}

func (s *ConfigurationStore) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.ConfigurationStoreType)
	return metadataInfo
}

func (s *ConfigurationStore) Close() error {
	s.mu.Lock()
	s.closed.Store(true)
	for id, sub := range s.subscriptions {
		if err := s.informer.RemoveEventHandler(sub.registration); err != nil {
			s.logger.Errorf("failed to remove event handler for subscription %s: %v", id, err)
		}
		sub.cancel()
		delete(s.subscriptions, id)
	}
	s.mu.Unlock()

	if s.informerCancel != nil {
		s.informerCancel()
	}
	s.informerWg.Wait()

	return nil
}

// startInformer creates and starts a SharedIndexInformer that watches the
// configured ConfigMap. Per-subscriber handlers are added later via Subscribe.
func (s *ConfigurationStore) startInformer(ctx context.Context) error {
	informerCtx, cancel := context.WithCancel(context.Background())
	s.informerCancel = cancel

	var resyncPeriod time.Duration
	if s.metadata.ResyncPeriod != nil {
		resyncPeriod = *s.metadata.ResyncPeriod
	}

	fieldSelector := fields.OneTermEqualSelector("metadata.name", s.metadata.ConfigMapName).String()
	watchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return s.kubeClient.CoreV1().ConfigMaps(s.namespace).List(informerCtx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return s.kubeClient.CoreV1().ConfigMaps(s.namespace).Watch(informerCtx, options)
		},
	}

	s.informer = cache.NewSharedIndexInformer(watchlist, &corev1.ConfigMap{}, resyncPeriod, cache.Indexers{})

	s.informerWg.Add(1)
	go func() {
		defer s.informerWg.Done()
		s.informer.Run(informerCtx.Done())
		s.logger.Info("ConfigMap informer stopped")
	}()

	// Use Init's context for the sync check — it provides a timeout bound.
	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		cancel()
		s.informerWg.Wait()
		return fmt.Errorf("failed to sync informer cache for ConfigMap %q", s.metadata.ConfigMapName)
	}

	return nil
}

// subscriberHandler implements cache.ResourceEventHandler for a single
// subscriber. The informer calls these methods directly — OnAdd is also used
// for initial state replay when the handler is registered on a synced informer.
type subscriberHandler struct {
	id      string
	keys    []string // empty means all keys
	handler configuration.UpdateHandler
	ctx     context.Context
	logger  logger.Logger
}

func (h *subscriberHandler) OnAdd(obj any, _ bool) {
	cm := extractConfigMap(obj)
	if cm == nil {
		return
	}

	items := filterByKeys(configMapToItems(cm), h.keys)
	if len(items) == 0 {
		return
	}
	h.send(items)
}

func (h *subscriberHandler) OnUpdate(oldObj, newObj any) {
	oldCM := extractConfigMap(oldObj)
	newCM := extractConfigMap(newObj)
	if oldCM == nil || newCM == nil {
		return
	}

	if oldCM.ResourceVersion == newCM.ResourceVersion {
		return
	}

	changed := computeChangedItems(oldCM, newCM)
	items := filterByKeys(changed, h.keys)
	if len(items) == 0 {
		return
	}
	h.send(items)
}

func (h *subscriberHandler) OnDelete(obj any) {
	cm := extractConfigMap(obj)
	if cm == nil {
		return
	}

	items := filterByKeys(deletedItems(cm), h.keys)
	if len(items) == 0 {
		return
	}
	h.send(items)
}

func (h *subscriberHandler) send(items map[string]*configuration.Item) {
	if h.ctx.Err() != nil {
		return
	}
	if err := h.handler(h.ctx, &configuration.UpdateEvent{
		ID:    h.id,
		Items: items,
	}); err != nil {
		h.logger.Errorf("subscription %s handler error: %v", h.id, err)
	}
}

// extractConfigMap extracts a *corev1.ConfigMap from an informer event object,
// handling the cache.DeletedFinalStateUnknown wrapper.
func extractConfigMap(obj any) *corev1.ConfigMap {
	switch t := obj.(type) {
	case *corev1.ConfigMap:
		return t
	case cache.DeletedFinalStateUnknown:
		if cm, ok := t.Obj.(*corev1.ConfigMap); ok {
			return cm
		}
	}
	return nil
}

// configMapToItems builds configuration items from all data in a ConfigMap.
func configMapToItems(cm *corev1.ConfigMap) map[string]*configuration.Item {
	items := make(map[string]*configuration.Item, len(cm.Data)+len(cm.BinaryData))
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
	return items
}

// deletedItems builds configuration items with "deleted" metadata for every
// key in the given ConfigMap.
func deletedItems(cm *corev1.ConfigMap) map[string]*configuration.Item {
	items := make(map[string]*configuration.Item, len(cm.Data)+len(cm.BinaryData))
	for k := range cm.Data {
		items[k] = &configuration.Item{
			Value:    "",
			Version:  cm.ResourceVersion,
			Metadata: map[string]string{"deleted": "true"},
		}
	}
	for k := range cm.BinaryData {
		items[k] = &configuration.Item{
			Value:    "",
			Version:  cm.ResourceVersion,
			Metadata: map[string]string{"deleted": "true"},
		}
	}
	return items
}

// filterByKeys returns only the items matching the given keys.
// If keys is empty, all items are returned.
func filterByKeys(items map[string]*configuration.Item, keys []string) map[string]*configuration.Item {
	if len(keys) == 0 {
		return items
	}
	filtered := make(map[string]*configuration.Item, len(keys))
	for _, k := range keys {
		if item, ok := items[k]; ok {
			filtered[k] = item
		}
	}
	return filtered
}

// computeChangedItems computes the set of configuration items that changed
// between oldCM and newCM across both Data and BinaryData.
func computeChangedItems(oldCM, newCM *corev1.ConfigMap) map[string]*configuration.Item {
	changedItems := make(map[string]*configuration.Item)

	for k, newVal := range newCM.Data {
		oldVal, existed := oldCM.Data[k]
		if !existed || oldVal != newVal {
			changedItems[k] = &configuration.Item{
				Value:    newVal,
				Version:  newCM.ResourceVersion,
				Metadata: map[string]string{},
			}
		}
	}

	for k, newVal := range newCM.BinaryData {
		oldVal, existed := oldCM.BinaryData[k]
		if !existed || !bytes.Equal(oldVal, newVal) {
			changedItems[k] = &configuration.Item{
				Value:    base64.StdEncoding.EncodeToString(newVal),
				Version:  newCM.ResourceVersion,
				Metadata: map[string]string{"encoding": "base64"},
			}
		}
	}

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
			changedItems[k] = &configuration.Item{
				Value:    "",
				Version:  newCM.ResourceVersion,
				Metadata: map[string]string{"deleted": "true"},
			}
		}
	}

	return changedItems
}

func resolveNamespace() string {
	if ns, ok := os.LookupEnv("NAMESPACE"); ok {
		return ns
	}
	return "default"
}
