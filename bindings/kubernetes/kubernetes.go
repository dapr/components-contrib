// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	kubeclient "github.com/dapr/components-contrib/authentication/kubernetes"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type kubernetesInput struct {
	kubeClient        kubernetes.Interface
	namespace         string
	resyncPeriodInSec time.Duration // nolint:stylecheck
	logger            logger.Logger
}

type EventResponse struct {
	Event  string   `json:"event"`
	OldVal v1.Event `json:"oldVal"`
	NewVal v1.Event `json:"newVal"`
}

var _ = bindings.InputBinding(&kubernetesInput{})

// NewKubernetes returns a new Kubernetes event input binding
func NewKubernetes(logger logger.Logger) bindings.InputBinding {
	return &kubernetesInput{logger: logger}
}

func (k *kubernetesInput) Init(metadata bindings.Metadata) error {
	client, err := kubeclient.GetKubeClient()
	if err != nil {
		return err
	}
	k.kubeClient = client

	return k.parseMetadata(metadata)
}

func (k *kubernetesInput) parseMetadata(metadata bindings.Metadata) error {
	if val, ok := metadata.Properties["namespace"]; ok && val != "" {
		k.namespace = val
	} else {
		return errors.New("namespace is missing in metadata")
	}
	if val, ok := metadata.Properties["resyncPeriodInSec"]; ok && val != "" {
		intval, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			k.logger.Warnf("invalid resyncPeriodInSec %s; %v; defaulting to 10s", val, err)
			k.resyncPeriodInSec = time.Second * 10
		} else {
			k.resyncPeriodInSec = time.Second * time.Duration(intval)
		}
	}

	return nil
}

func (k *kubernetesInput) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	watchlist := cache.NewListWatchFromClient(
		k.kubeClient.CoreV1().RESTClient(),
		"events",
		k.namespace,
		fields.Everything())
	var resultChan chan EventResponse = make(chan EventResponse)
	_, controller := cache.NewInformer(
		watchlist,
		&v1.Event{},
		k.resyncPeriodInSec,
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
	stopCh := make(chan struct{})
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	go controller.Run(stopCh)
	done := false
	for !done {
		select {
		case obj := <-resultChan:
			data, err := json.Marshal(obj)
			if err != nil {
				k.logger.Errorf("Error marshalling event %w", err)
			} else {
				handler(&bindings.ReadResponse{
					Data: data,
				})
			}
		case <-sigterm:
			done = true
			close(stopCh)
		}
	}

	return nil
}
