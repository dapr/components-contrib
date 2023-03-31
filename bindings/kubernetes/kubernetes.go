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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/dapr/components-contrib/bindings"
	kubeclient "github.com/dapr/components-contrib/internal/authentication/kubernetes"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type kubernetesInput struct {
	kubeClient   kubernetes.Interface
	namespace    string
	resyncPeriod time.Duration
	logger       logger.Logger
	closed       atomic.Bool
	closeCh      chan struct{}
	wg           sync.WaitGroup
}

type EventResponse struct {
	Event  string   `json:"event"`
	OldVal v1.Event `json:"oldVal"`
	NewVal v1.Event `json:"newVal"`
}

type kubernetesMetadata struct {
	Namespace    string         `mapstructure:"namespace"`
	ResyncPeriod *time.Duration `mapstructure:"resyncPeriodInSec"`
}

// NewKubernetes returns a new Kubernetes event input binding.
func NewKubernetes(logger logger.Logger) bindings.InputBinding {
	return &kubernetesInput{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (k *kubernetesInput) Init(ctx context.Context, metadata bindings.Metadata) error {
	client, err := kubeclient.GetKubeClient()
	if err != nil {
		return err
	}
	k.kubeClient = client

	return k.parseMetadata(metadata)
}

func (k *kubernetesInput) parseMetadata(meta bindings.Metadata) error {
	m := kubernetesMetadata{}
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		if strings.Contains(err.Error(), "decoding 'resyncPeriodInSec': time: invalid duration") {
			k.logger.Warnf("invalid resyncPeriodInSec; %v; defaulting to 10s", err)
			m.ResyncPeriod = ptr.Of(time.Second * 10)
		} else {
			return err
		}
	}
	k.resyncPeriod = *m.ResyncPeriod

	if m.Namespace == "" {
		return errors.New("namespace is missing in metadata")
	}
	k.namespace = m.Namespace
	return nil
}

func (k *kubernetesInput) Read(ctx context.Context, handler bindings.Handler) error {
	if k.closed.Load() {
		return errors.New("binding is closed")
	}
	watchlist := cache.NewListWatchFromClient(
		k.kubeClient.CoreV1().RESTClient(),
		"events",
		k.namespace,
		fields.Everything(),
	)
	resultChan := make(chan EventResponse)
	_, controller := cache.NewInformer(
		watchlist,
		&v1.Event{},
		k.resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if obj != nil {
					resultChan <- EventResponse{
						Event:  "add",
						NewVal: *(obj.(*v1.Event)),
						OldVal: v1.Event{},
					}
				} else {
					k.logger.Warnf("Nil Object in Add handle %v", obj)
				}
			},
			DeleteFunc: func(obj interface{}) {
				if obj != nil {
					resultChan <- EventResponse{
						Event:  "delete",
						OldVal: *(obj.(*v1.Event)),
						NewVal: v1.Event{},
					}
				} else {
					k.logger.Warnf("Nil Object in Delete handle %v", obj)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				if oldObj != nil && newObj != nil {
					resultChan <- EventResponse{
						Event:  "update",
						OldVal: *(oldObj.(*v1.Event)),
						NewVal: *(newObj.(*v1.Event)),
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

	// Start the controller in backgound
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
func (k *kubernetesInput) GetComponentMetadata() map[string]string {
	metadataStruct := kubernetesMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.ComponentType.BindingType)
	return metadataInfo
}
