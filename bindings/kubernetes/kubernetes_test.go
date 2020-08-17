// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	nsName := "fooNamespace"
	t.Run("parse metadata", func(t *testing.T) {
		resyncPeriod := time.Second * 15
		m := bindings.Metadata{}
		m.Properties = map[string]string{"namespace": nsName, "resyncPeriodInSec": "15"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		i.parseMetadata(m)

		assert.Equal(t, nsName, i.namespace, "The namespaces should be the same.")
		assert.Equal(t, resyncPeriod, i.resyncPeriodInSec, "The resyncPeriod should be the same.")
	})
	t.Run("parse metadata no namespace", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"resyncPeriodInSec": "15"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		err := i.parseMetadata(m)

		assert.NotNil(t, err, "Expected err to be returned.")
		assert.Equal(t, "namespace is missing in metadata", err.Error(), "Error message not same.")
	})
	t.Run("parse metadata invalid resync period", func(t *testing.T) {
		m := bindings.Metadata{}
		m.Properties = map[string]string{"namespace": nsName, "resyncPeriodInSec": "invalid"}

		i := kubernetesInput{logger: logger.NewLogger("test")}
		err := i.parseMetadata(m)

		assert.Nil(t, err, "Expected err to be nil.")
		assert.Equal(t, nsName, i.namespace, "The namespaces should be the same.")
		assert.Equal(t, time.Second*10, i.resyncPeriodInSec, "The resyncPeriod should be the same.")
	})
}

// func TestReadItem(t *testing.T) {
// 	fakeClient := ktesting.NewFakeControllerSource()

// 	i := &kubernetesInput{
// 		kubeClient:        fakeClient,
// 		resyncPeriodInSec: time.Millisecond,
// 		namespace:         "foo",
// 		logger:            logger.NewLogger("test"),
// 	}
// 	er := EventResponse{}
// 	data := []byte(`{"event":"add","oldVal":{"metadata":{"creationTimestamp":null},"involvedObject":{},"source":{},"firstTimestamp":null,"lastTimestamp":null,"eventTime":null,"reportingComponent":"","reportingInstance":""},"newVal":{"metadata":{"name":"redis-slave.162c1a8e548e0c99","namespace":"default","selfLink":"/api/v1/namespaces/default/events/redis-slave.162c1a8e548e0c99","uid":"70cf7cf8-18cc-45c5-bcab-68776ad5efa3","resourceVersion":"662947","creationTimestamp":"2020-08-17T16:19:26Z","managedFields":[{"manager":"kube-controller-manager","operation":"Update","apiVersion":"v1","time":"2020-08-17T16:19:26Z","fieldsType":"FieldsV1","fieldsV1":{"f:count":{},"f:firstTimestamp":{},"f:involvedObject":{"f:apiVersion":{},"f:kind":{},"f:name":{},"f:namespace":{},"f:resourceVersion":{},"f:uid":{}},"f:lastTimestamp":{},"f:message":{},"f:reason":{},"f:source":{"f:component":{}},"f:type":{}}}]},"involvedObject":{"kind":"Service","namespace":"default","name":"redis-slave","uid":"5d79e7fd-45b2-47a9-9b49-89e694fb7e9a","apiVersion":"v1","resourceVersion":"233622"},"reason":"FailedToUpdateEndpointSlices","message":"Error updating Endpoint Slices for Service default/redis-slave: Error deleting redis-slave-679x4 EndpointSlice for Service default/redis-slave: endpointslices.discovery.k8s.io \"redis-slave-679x4\" not found","source":{"component":"endpoint-slice-controller"},"firstTimestamp":"2020-08-17T16:19:26Z","lastTimestamp":"2020-08-17T16:19:26Z","count":1,"type":"Warning","eventTime":null,"reportingComponent":"","reportingInstance":""}}`)
// 	err := json.Unmarshal(data, &er)
// 	assert.Nil(t, err, "Unexpected err")

// 	ev := er.NewVal
// 	fakeClient.Add(ev)
// 	// ev.ObjectMeta.Namespace = "defaul"
// 	assert.NotNil(t, fakeClient, "fakeClient not nil")
// 	cv1 := fakeClient.CoreV1()
// 	assert.NotNil(t, cv1, "Unexpected err cv1")

// 	eapi := cv1.Events("default")
// 	assert.NotNil(t, eapi, "Unexpected err eapi")

// 	_, err = eapi.Create(&ev)

// 	assert.Nil(t, err)

// 	// // count := 0
// 	stopCh := make(chan struct{})
// 	var handler = func(res *bindings.ReadResponse) error {
// 		fmt.Println("In Response Handler")
// 		result := EventResponse{}
// 		assert.NotNil(t, res, "Expected read response to be not nil")
// 		assert.NotNil(t, res.Data, "Expected read response data to be not nil")
// 		json.Unmarshal(res.Data, &result)

// 		assert.Equal(t, "add", result.Event, "Unexpected watch event type: %v", result.Event)
// 		stopCh <- struct{}{}
// 		return nil
// 	}

// 	go i.Read(handler)
// 	<-stopCh
// 	// time.Sleep(5 * time.Second)
// 	// pid := syscall.Getpid()
// 	// proc, _ := os.FindProcess(pid)
// 	// proc.Signal(os.Interrupt)
// 	// assert.Equal(t, 1, count, "Expected 1 item, saw %v\n", count)
// }
