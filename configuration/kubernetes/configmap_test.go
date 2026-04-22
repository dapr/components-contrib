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
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// newTestStoreWithInformer creates a ConfigurationStore backed by a fake
// Kubernetes client and a real SharedIndexInformer. The informer is started
// and synced before the function returns.
func newTestStoreWithInformer(t *testing.T, objects ...corev1.ConfigMap) *ConfigurationStore {
	t.Helper()
	t.Setenv("NAMESPACE", "default")

	fakeClient := fake.NewSimpleClientset()
	for i := range objects {
		_, err := fakeClient.CoreV1().ConfigMaps(objects[i].Namespace).Create(
			t.Context(), &objects[i], metav1.CreateOptions{},
		)
		require.NoError(t, err)
	}

	store := &ConfigurationStore{
		kubeClient:    fakeClient,
		namespace:     "default",
		logger:        logger.NewLogger("test"),
		subscriptions: make(map[string]*subscription),
		metadata:      metadata{ConfigMapName: "my-config"},
	}

	require.NoError(t, store.startInformer(t.Context()))
	t.Cleanup(func() { store.Close() })

	return store
}

func testConfigMap() corev1.ConfigMap {
	return corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "my-config",
			Namespace:       "default",
			ResourceVersion: "100",
		},
		Data: map[string]string{
			"log.level":          "info",
			"feature.enable-v2":  "true",
			"database.pool-size": "10",
		},
	}
}

func TestNewKubernetesConfigMapStore(t *testing.T) {
	store := NewKubernetesConfigMapStore(logger.NewLogger("test"))
	assert.NotNil(t, store)
}

func TestMetadata_Parse(t *testing.T) {
	t.Run("valid metadata", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "my-config", m.ConfigMapName)
	})

	t.Run("missing configMapName returns error", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configMapName is required")
	})

	t.Run("dotted configMapName is valid (DNS subdomain)", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my.dotted.config",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "my.dotted.config", m.ConfigMapName)
	})

	t.Run("invalid configMapName is rejected", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "INVALID_NAME",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid Kubernetes resource name")
	})

	t.Run("resyncPeriod is parsed", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
					"resyncPeriod":  "10m",
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, m.ResyncPeriod)
		assert.Equal(t, 10*time.Minute, *m.ResyncPeriod)
	})

	t.Run("resyncPeriod nil when not set", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
				},
			},
		})
		require.NoError(t, err)
		assert.Nil(t, m.ResyncPeriod)
	})
}

func TestInit_ValidMetadata(t *testing.T) {
	t.Setenv("NAMESPACE", "default")
	fakeClient := fake.NewSimpleClientset()

	store := &ConfigurationStore{
		kubeClient:    fakeClient,
		logger:        logger.NewLogger("test"),
		subscriptions: make(map[string]*subscription),
	}

	err := store.Init(t.Context(), configuration.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"configMapName": "my-config",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-config", store.metadata.ConfigMapName)
	require.NoError(t, store.Close())
}

func TestInit_InvalidMetadata(t *testing.T) {
	store := &ConfigurationStore{
		logger:        logger.NewLogger("test"),
		subscriptions: make(map[string]*subscription),
	}

	err := store.Init(t.Context(), configuration.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "configMapName is required")
}

func TestGet_AllKeys(t *testing.T) {
	store := newTestStoreWithInformer(t, testConfigMap())

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Items, 3)
	assert.Equal(t, "info", resp.Items["log.level"].Value)
	assert.Equal(t, "true", resp.Items["feature.enable-v2"].Value)
	assert.Equal(t, "10", resp.Items["database.pool-size"].Value)
}

func TestGet_SpecificKeys(t *testing.T) {
	store := newTestStoreWithInformer(t, testConfigMap())

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Items, 1)
	assert.Equal(t, "info", resp.Items["log.level"].Value)
}

func TestGet_MissingKeys(t *testing.T) {
	store := newTestStoreWithInformer(t, testConfigMap())

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{"nonexistent.key"},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestGet_BinaryData(t *testing.T) {
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "my-config",
			Namespace:       "default",
			ResourceVersion: "100",
		},
		Data: map[string]string{
			"text-key": "hello",
		},
		BinaryData: map[string][]byte{
			"binary-key": {0x01, 0x02, 0x03},
		},
	}
	store := newTestStoreWithInformer(t, cm)

	t.Run("get all includes binary data", func(t *testing.T) {
		resp, err := store.Get(t.Context(), &configuration.GetRequest{
			Keys:     []string{},
			Metadata: map[string]string{},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Items, 2)

		assert.Equal(t, "hello", resp.Items["text-key"].Value)
		assert.Empty(t, resp.Items["text-key"].Metadata["encoding"])

		expectedB64 := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})
		assert.Equal(t, expectedB64, resp.Items["binary-key"].Value)
		assert.Equal(t, "base64", resp.Items["binary-key"].Metadata["encoding"])
	})

	t.Run("get specific binary key", func(t *testing.T) {
		resp, err := store.Get(t.Context(), &configuration.GetRequest{
			Keys:     []string{"binary-key"},
			Metadata: map[string]string{},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Items, 1)
		assert.Equal(t, "base64", resp.Items["binary-key"].Metadata["encoding"])
	})
}

func TestGet_EmptyConfigMap(t *testing.T) {
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-config",
			Namespace: "default",
		},
	}
	store := newTestStoreWithInformer(t, cm)

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestGet_ConfigMapNotFound(t *testing.T) {
	store := newTestStoreWithInformer(t) // no ConfigMap created

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestUnsubscribe_Valid(t *testing.T) {
	store := newTestStoreWithInformer(t, testConfigMap())

	subID, err := store.Subscribe(t.Context(), &configuration.SubscribeRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	}, func(_ context.Context, _ *configuration.UpdateEvent) error {
		return nil
	})
	require.NoError(t, err)

	err = store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: subID})
	require.NoError(t, err)

	// Second unsubscribe should fail.
	err = store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: subID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestUnsubscribe_InvalidID(t *testing.T) {
	store := newTestStoreWithInformer(t)

	err := store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: "nonexistent"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestClose_PreventsFurtherSubscriptions(t *testing.T) {
	store := newTestStoreWithInformer(t, testConfigMap())

	require.NoError(t, store.Close())

	_, err := store.Subscribe(t.Context(), &configuration.SubscribeRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	}, func(_ context.Context, _ *configuration.UpdateEvent) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestResolveNamespace(t *testing.T) {
	t.Run("uses NAMESPACE env var", func(t *testing.T) {
		t.Setenv("NAMESPACE", "env-ns")
		assert.Equal(t, "env-ns", resolveNamespace())
	})

	t.Run("defaults to 'default' when env var not set", func(t *testing.T) {
		assert.Equal(t, "default", resolveNamespace())
	})
}

func TestGetComponentMetadata(t *testing.T) {
	store := &ConfigurationStore{
		logger:        logger.NewLogger("test"),
		subscriptions: make(map[string]*subscription),
	}
	metadataInfo := store.GetComponentMetadata()
	assert.NotNil(t, metadataInfo)
}

func TestComputeChangedItems(t *testing.T) {
	t.Run("detects added and modified keys", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old-val"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new-val", "key2": "added"},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 2)
		assert.Equal(t, "new-val", items["key1"].Value)
		assert.Equal(t, "added", items["key2"].Value)
		assert.Equal(t, "2", items["key1"].Version)
	})

	t.Run("detects deleted keys with metadata", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1", "key2": "val2"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "val1"},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 1)
		assert.Equal(t, "", items["key2"].Value)
		assert.Equal(t, "true", items["key2"].Metadata["deleted"])
	})

	t.Run("no changes when data is identical", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1"},
		}

		items := computeChangedItems(cm, cm)

		assert.Empty(t, items)
	})

	t.Run("detects binaryData changes", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"bin-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			BinaryData: map[string][]byte{"bin-key": {0x01, 0x02}},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 1)
		expected := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02})
		assert.Equal(t, expected, items["bin-key"].Value)
		assert.Equal(t, "base64", items["bin-key"].Metadata["encoding"])
	})

	t.Run("detects deleted binaryData keys", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"bin-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			BinaryData: map[string][]byte{},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Equal(t, "", items["bin-key"].Value)
		assert.Equal(t, "true", items["bin-key"].Metadata["deleted"])
	})

	t.Run("key moved from binaryData to data is not marked deleted", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"shared-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"shared-key": "now-text"},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 1)
		assert.Equal(t, "now-text", items["shared-key"].Value)
		assert.Empty(t, items["shared-key"].Metadata["deleted"])
	})
}

func TestSubscriberHandler_OnAdd(t *testing.T) {
	t.Run("delivers all keys", func(t *testing.T) {
		receivedCh := make(chan *configuration.UpdateEvent, 1)
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error { receivedCh <- e; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnAdd(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1", "key2": "val2"},
		}, false)

		select {
		case e := <-receivedCh:
			assert.Len(t, e.Items, 2)
			assert.Equal(t, "val1", e.Items["key1"].Value)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	})

	t.Run("filters by subscribed keys", func(t *testing.T) {
		receivedCh := make(chan *configuration.UpdateEvent, 1)
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{"key1"},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error { receivedCh <- e; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnAdd(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1", "key2": "val2"},
		}, false)

		select {
		case e := <-receivedCh:
			assert.Len(t, e.Items, 1)
			assert.Equal(t, "val1", e.Items["key1"].Value)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	})

	t.Run("skips when no matching keys", func(t *testing.T) {
		handlerCalled := false
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{"nonexistent"},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error { handlerCalled = true; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnAdd(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1"},
		}, false)

		assert.False(t, handlerCalled)
	})

	t.Run("skips cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		handlerCalled := false
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error { handlerCalled = true; return nil },
			ctx:     ctx,
			logger:  logger.NewLogger("test"),
		}

		h.OnAdd(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1"},
		}, false)

		assert.False(t, handlerCalled)
	})
}

func TestSubscriberHandler_OnUpdate(t *testing.T) {
	t.Run("delivers changed keys", func(t *testing.T) {
		receivedCh := make(chan *configuration.UpdateEvent, 1)
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error { receivedCh <- e; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnUpdate(
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}, Data: map[string]string{"key1": "old"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}, Data: map[string]string{"key1": "new"}},
		)

		select {
		case e := <-receivedCh:
			assert.Equal(t, "new", e.Items["key1"].Value)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	})

	t.Run("skips same ResourceVersion", func(t *testing.T) {
		handlerCalled := false
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error { handlerCalled = true; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}, Data: map[string]string{"key1": "val"}}
		h.OnUpdate(cm, cm)

		assert.False(t, handlerCalled)
	})

	t.Run("filters by subscribed keys", func(t *testing.T) {
		handlerCalled := false
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{"key2"},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error { handlerCalled = true; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnUpdate(
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}, Data: map[string]string{"key1": "old", "key2": "same"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}, Data: map[string]string{"key1": "new", "key2": "same"}},
		)

		// key2 didn't change, so handler should not be called.
		assert.False(t, handlerCalled)
	})
}

func TestSubscriberHandler_OnDelete(t *testing.T) {
	t.Run("delivers deleted markers", func(t *testing.T) {
		receivedCh := make(chan *configuration.UpdateEvent, 1)
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error { receivedCh <- e; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnDelete(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"},
			Data:       map[string]string{"key1": "val1", "key2": "val2"},
		})

		select {
		case e := <-receivedCh:
			assert.Len(t, e.Items, 2)
			assert.Equal(t, "true", e.Items["key1"].Metadata["deleted"])
			assert.Equal(t, "", e.Items["key1"].Value)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	})

	t.Run("handles DeletedFinalStateUnknown", func(t *testing.T) {
		receivedCh := make(chan *configuration.UpdateEvent, 1)
		h := &subscriberHandler{
			id:      "sub-1",
			keys:    []string{},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error { receivedCh <- e; return nil },
			ctx:     t.Context(),
			logger:  logger.NewLogger("test"),
		}

		h.OnDelete(cache.DeletedFinalStateUnknown{
			Key: "default/my-config",
			Obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"},
				Data:       map[string]string{"key1": "val1"},
			},
		})

		select {
		case e := <-receivedCh:
			assert.Len(t, e.Items, 1)
			assert.Equal(t, "true", e.Items["key1"].Metadata["deleted"])
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	})
}

func TestExtractConfigMap(t *testing.T) {
	t.Run("direct ConfigMap pointer", func(t *testing.T) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
		assert.Equal(t, cm, extractConfigMap(cm))
	})

	t.Run("DeletedFinalStateUnknown with ConfigMap", func(t *testing.T) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
		result := extractConfigMap(cache.DeletedFinalStateUnknown{Obj: cm})
		assert.Equal(t, cm, result)
	})

	t.Run("non-ConfigMap returns nil", func(t *testing.T) {
		assert.Nil(t, extractConfigMap("not-a-configmap"))
	})

	t.Run("DeletedFinalStateUnknown with non-ConfigMap returns nil", func(t *testing.T) {
		assert.Nil(t, extractConfigMap(cache.DeletedFinalStateUnknown{Obj: "not-a-configmap"}))
	})
}

func TestFilterByKeys(t *testing.T) {
	items := map[string]*configuration.Item{
		"key1": {Value: "val1"},
		"key2": {Value: "val2"},
		"key3": {Value: "val3"},
	}

	t.Run("empty keys returns all items", func(t *testing.T) {
		result := filterByKeys(items, []string{})
		assert.Len(t, result, 3)
	})

	t.Run("filters to specified keys", func(t *testing.T) {
		result := filterByKeys(items, []string{"key1", "key3"})
		assert.Len(t, result, 2)
		assert.Contains(t, result, "key1")
		assert.Contains(t, result, "key3")
	})

	t.Run("nonexistent keys are omitted", func(t *testing.T) {
		result := filterByKeys(items, []string{"nonexistent"})
		assert.Empty(t, result)
	})
}

func TestSubscribe_InitialState(t *testing.T) {
	store := newTestStoreWithInformer(t, testConfigMap())

	receivedCh := make(chan *configuration.UpdateEvent, 1)
	_, err := store.Subscribe(t.Context(), &configuration.SubscribeRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	}, func(_ context.Context, e *configuration.UpdateEvent) error {
		receivedCh <- e
		return nil
	})
	require.NoError(t, err)

	select {
	case received := <-receivedCh:
		require.NotNil(t, received)
		assert.Equal(t, "info", received.Items["log.level"].Value)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial state delivery")
	}
}
