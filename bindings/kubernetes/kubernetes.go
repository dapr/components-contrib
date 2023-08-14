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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/dapr/components-contrib/bindings"
	kubeclient "github.com/dapr/components-contrib/internal/authentication/kubernetes"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type kubernetesInput struct {
	metadata   kubernetesMetadata
	kubeClient kubernetes.Interface
	logger     logger.Logger
	closed     atomic.Bool
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

type EventResponse struct {
	Event  string       `json:"event"`
	OldVal corev1.Event `json:"oldVal"`
	NewVal corev1.Event `json:"newVal"`
}

type kubernetesMetadata struct {
	Namespace      string        `mapstructure:"namespace"`
	KubeconfigPath string        `mapstructure:"kubeconfigPath"`
	ResyncPeriod   time.Duration `mapstructure:"resyncPeriod" mapstructurealiases:"resyncPeriodInSec"`
}

// NewKubernetes returns a new Kubernetes event input binding.
func NewKubernetes(logger logger.Logger) bindings.InputBinding {
	return &kubernetesInput{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (k *kubernetesInput) Init(ctx context.Context, metadata bindings.Metadata) error {
	err := k.parseMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	kubeconfigPath := k.metadata.KubeconfigPath
	if kubeconfigPath == "" {
		kubeconfigPath = kubeclient.GetKubeconfigPath(k.logger, os.Args)
	}

	client, err := kubeclient.GetKubeClient(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to initialize Kubernetes client: %w", err)
	}
	k.kubeClient = client

	return nil
}

func (k *kubernetesInput) parseMetadata(meta bindings.Metadata) error {
	// Set default values
	k.metadata = kubernetesMetadata{
		ResyncPeriod: 10 * time.Second,
	}

	// Decode
	err := metadata.DecodeMetadata(meta.Properties, &k.metadata)
	if err != nil {
		return err
	}

	// Validate
	if k.metadata.Namespace == "" {
		return errors.New("namespace is missing in metadata")
	}

	return nil
}

func (k *kubernetesInput) Read(ctx context.Context, handler bindings.Handler) error {
	if k.closed.Load() {
		return errors.New("binding is closed")
	}
	watchlist := cache.NewListWatchFromClient(
		k.kubeClient.CoreV1().RESTClient(),
		"events",
		k.metadata.Namespace,
		fields.Everything(),
	)
	resultChan := make(chan EventResponse)
	_, controller := cache.NewInformer(
		watchlist,
		&corev1.Event{},
		k.metadata.ResyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if obj != nil {
					resultChan <- EventResponse{
						Event:  "add",
						NewVal: *(obj.(*corev1.Event)),
						OldVal: corev1.Event{},
					}
				} else {
					k.logger.Warnf("Nil Object in Add handle %v", obj)
				}
			},
			DeleteFunc: func(obj interface{}) {
				if obj != nil {
					resultChan <- EventResponse{
						Event:  "delete",
						OldVal: *(obj.(*corev1.Event)),
						NewVal: corev1.Event{},
					}
				} else {
					k.logger.Warnf("Nil Object in Delete handle %v", obj)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				if oldObj != nil && newObj != nil {
					resultChan <- EventResponse{
						Event:  "update",
						OldVal: *(oldObj.(*corev1.Event)),
						NewVal: *(newObj.(*corev1.Event)),
					}
				} else {
					k.logger.Warnf("Nil Objects in Update handle %v %v", oldObj, newObj)
				}
			},
		},
	)

	k.wg.Add(3)
	readCtx, cancel := context.WithCancel(ctx)

	// catch when binding is closed.
	go func() {
		defer k.wg.Done()
		defer cancel()
		select {
		case <-readCtx.Done():
		case <-k.closeCh:
		}
	}()

	// Start the controller in background
	go func() {
		defer k.wg.Done()
		controller.Run(readCtx.Done())
	}()

	// Watch for new messages and for context cancellation
	go func() {
		defer k.wg.Done()
		var (
			obj  EventResponse
			data []byte
			err  error
		)
		for {
			select {
			case obj = <-resultChan:
				data, err = json.Marshal(obj)
				if err != nil {
					k.logger.Errorf("Error marshalling event %w", err)
				} else {
					handler(ctx, &bindings.ReadResponse{
						Data: data,
					})
				}
			case <-readCtx.Done():
				return
			}
		}
	}()

	return nil
}

func (k *kubernetesInput) Close() error {
	if k.closed.CompareAndSwap(false, true) {
		close(k.closeCh)
	}
	k.wg.Wait()
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (k *kubernetesInput) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := kubernetesMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
