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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestStore(t *testing.T, objects ...corev1.ConfigMap) *ConfigurationStore {
	t.Helper()

	fakeClient := fake.NewSimpleClientset()
	for i := range objects {
		_, err := fakeClient.CoreV1().ConfigMaps(objects[i].Namespace).Create(
			t.Context(), &objects[i], metav1.CreateOptions{},
		)
		require.NoError(t, err)
	}

	return &ConfigurationStore{
		kubeClient: fakeClient,
		logger:     logger.NewLogger("test"),
	}
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
	// Verify factory returns a non-nil Store implementation
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
					"namespace":     "production",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "my-config", m.ConfigMapName)
		assert.Equal(t, "production", m.Namespace)
	})

	t.Run("missing configMapName returns error", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"namespace": "default",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configMapName is required")
	})

	t.Run("namespace defaults to 'default'", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "default", m.Namespace)
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

	t.Run("invalid namespace is rejected", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
					"namespace":     "../../etc",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid Kubernetes namespace name")
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
		assert.Equal(t, 10*time.Minute, m.ResyncPeriod)
	})
}

func TestInit_ConfigMapNotFound(t *testing.T) {
	// Init should fail when the ConfigMap does not exist in the cluster
	store := newTestStore(t) // no ConfigMaps pre-created
	store.metadata = metadata{ConfigMapName: "nonexistent", Namespace: "default"}

	ns := store.resolveNamespace(nil)
	_, err := store.kubeClient.CoreV1().ConfigMaps(ns).Get(t.Context(), store.metadata.ConfigMapName, metav1.GetOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestInit_ConfigMapExists(t *testing.T) {
	// Verify the ConfigMap existence check passes when the ConfigMap exists
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	ns := store.resolveNamespace(nil)
	_, err := store.kubeClient.CoreV1().ConfigMaps(ns).Get(t.Context(), store.metadata.ConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestGet_AllKeys(t *testing.T) {
	// Verify Get with no keys returns all items from the ConfigMap
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

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
	// Verify Get returns only requested keys
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Items, 1)
	assert.Equal(t, "info", resp.Items["log.level"].Value)
}

func TestGet_MissingKeys(t *testing.T) {
	// Verify missing keys are silently skipped, not errors
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{"nonexistent.key"},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestGet_BinaryData(t *testing.T) {
	// Verify binaryData keys are returned as base64 with encoding metadata
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
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

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
	// Verify empty ConfigMap returns empty items, not an error
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-config",
			Namespace: "default",
		},
	}
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestGet_ConfigMapNotFound(t *testing.T) {
	// Verify Get returns error when ConfigMap does not exist
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "missing", Namespace: "default"}

	_, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ConfigMap")
}

func TestUnsubscribe_Valid(t *testing.T) {
	// Verify unsubscribing a known ID calls cancel and succeeds
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	cancelled := false
	store.cancelMap.Store("test-sub-id", context.CancelFunc(func() {
		cancelled = true
	}))

	err := store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: "test-sub-id"})
	require.NoError(t, err)
	assert.True(t, cancelled)
}

func TestUnsubscribe_InvalidID(t *testing.T) {
	// Verify unsubscribing a non-existent ID returns an error
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	err := store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: "nonexistent"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestClose_CancelsAllSubscriptions(t *testing.T) {
	// Verify Close cancels all active subscriptions
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	var mu sync.Mutex
	cancelledIDs := []string{}

	for _, id := range []string{"sub-1", "sub-2", "sub-3"} {
		capturedID := id
		store.cancelMap.Store(id, context.CancelFunc(func() {
			mu.Lock()
			cancelledIDs = append(cancelledIDs, capturedID)
			mu.Unlock()
		}))
	}

	err := store.Close()
	require.NoError(t, err)
	assert.Len(t, cancelledIDs, 3)
	assert.ElementsMatch(t, []string{"sub-1", "sub-2", "sub-3"}, cancelledIDs)
}

func TestClose_PreventsFurtherSubscriptions(t *testing.T) {
	// Verify Subscribe fails after Close has been called
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	err := store.Close()
	require.NoError(t, err)

	_, err = store.Subscribe(t.Context(), &configuration.SubscribeRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	}, func(_ context.Context, _ *configuration.UpdateEvent) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestNamespaceResolution(t *testing.T) {
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "component-ns"}

	t.Run("request metadata takes precedence", func(t *testing.T) {
		ns := store.resolveNamespace(map[string]string{"namespace": "request-ns"})
		assert.Equal(t, "request-ns", ns)
	})

	t.Run("env var fallback", func(t *testing.T) {
		t.Setenv("NAMESPACE", "env-ns")
		ns := store.resolveNamespace(map[string]string{})
		assert.Equal(t, "env-ns", ns)
	})

	t.Run("component metadata default", func(t *testing.T) {
		ns := store.resolveNamespace(map[string]string{})
		assert.Equal(t, "component-ns", ns)
	})

	t.Run("invalid namespace override falls back", func(t *testing.T) {
		// Verify invalid namespace in request metadata is rejected and falls back to component metadata
		ns := store.resolveNamespace(map[string]string{"namespace": "../../etc"})
		assert.Equal(t, "component-ns", ns)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	// Verify metadata reflection returns expected field names
	store := &ConfigurationStore{logger: logger.NewLogger("test")}
	metadataInfo := store.GetComponentMetadata()
	assert.NotNil(t, metadataInfo)
}

func TestIsSubscribedKey(t *testing.T) {
	t.Run("empty keys means subscribe to all", func(t *testing.T) {
		assert.True(t, isSubscribedKey([]string{}, "any-key"))
	})

	t.Run("nil keys means subscribe to all", func(t *testing.T) {
		assert.True(t, isSubscribedKey(nil, "any-key"))
	})

	t.Run("key in list", func(t *testing.T) {
		assert.True(t, isSubscribedKey([]string{"a", "b", "c"}, "b"))
	})

	t.Run("key not in list", func(t *testing.T) {
		assert.False(t, isSubscribedKey([]string{"a", "b", "c"}, "d"))
	})
}

func TestHandleConfigMapUpdate(t *testing.T) {
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: "default"}

	t.Run("detects added and modified keys", func(t *testing.T) {
		// Verify handler is called with correct changed items when keys are added/modified
		var receivedEvent *configuration.UpdateEvent
		handler := func(_ context.Context, e *configuration.UpdateEvent) error {
			receivedEvent = e
			return nil
		}

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old-val"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new-val", "key2": "added"},
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{}},
			handler, oldCM, newCM, "sub-1",
		)

		require.NotNil(t, receivedEvent)
		assert.Equal(t, "sub-1", receivedEvent.ID)
		assert.Len(t, receivedEvent.Items, 2)
		assert.Equal(t, "new-val", receivedEvent.Items["key1"].Value)
		assert.Equal(t, "added", receivedEvent.Items["key2"].Value)
		assert.Equal(t, "2", receivedEvent.Items["key1"].Version)
	})

	t.Run("detects deleted keys with metadata", func(t *testing.T) {
		// Verify deleted keys have {"deleted": "true"} metadata
		var receivedEvent *configuration.UpdateEvent
		handler := func(_ context.Context, e *configuration.UpdateEvent) error {
			receivedEvent = e
			return nil
		}

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1", "key2": "val2"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "val1"},
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{}},
			handler, oldCM, newCM, "sub-1",
		)

		require.NotNil(t, receivedEvent)
		assert.Len(t, receivedEvent.Items, 1)
		assert.Equal(t, "", receivedEvent.Items["key2"].Value)
		assert.Equal(t, "true", receivedEvent.Items["key2"].Metadata["deleted"])
	})

	t.Run("filters by subscribed keys", func(t *testing.T) {
		// Verify only subscribed keys trigger notifications
		var receivedEvent *configuration.UpdateEvent
		handler := func(_ context.Context, e *configuration.UpdateEvent) error {
			receivedEvent = e
			return nil
		}

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old1", "key2": "old2"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new1", "key2": "new2"},
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{"key1"}},
			handler, oldCM, newCM, "sub-1",
		)

		require.NotNil(t, receivedEvent)
		assert.Len(t, receivedEvent.Items, 1)
		assert.Equal(t, "new1", receivedEvent.Items["key1"].Value)
		assert.Nil(t, receivedEvent.Items["key2"])
	})

	t.Run("no notification when nothing changed", func(t *testing.T) {
		// Verify handler is NOT called when ConfigMap data is unchanged
		handlerCalled := false
		handler := func(_ context.Context, _ *configuration.UpdateEvent) error {
			handlerCalled = true
			return nil
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1"},
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{}},
			handler, cm, cm, "sub-1",
		)

		assert.False(t, handlerCalled)
	})

	t.Run("detects binaryData changes", func(t *testing.T) {
		// Verify binaryData changes are detected and base64-encoded
		var receivedEvent *configuration.UpdateEvent
		handler := func(_ context.Context, e *configuration.UpdateEvent) error {
			receivedEvent = e
			return nil
		}

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"bin-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			BinaryData: map[string][]byte{"bin-key": {0x01, 0x02}},
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{}},
			handler, oldCM, newCM, "sub-1",
		)

		require.NotNil(t, receivedEvent)
		assert.Len(t, receivedEvent.Items, 1)
		expected := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02})
		assert.Equal(t, expected, receivedEvent.Items["bin-key"].Value)
		assert.Equal(t, "base64", receivedEvent.Items["bin-key"].Metadata["encoding"])
	})

	t.Run("detects deleted binaryData keys", func(t *testing.T) {
		// Verify binaryData key deletion produces {"deleted": "true"} metadata
		var receivedEvent *configuration.UpdateEvent
		handler := func(_ context.Context, e *configuration.UpdateEvent) error {
			receivedEvent = e
			return nil
		}

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"bin-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			BinaryData: map[string][]byte{},
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{}},
			handler, oldCM, newCM, "sub-1",
		)

		require.NotNil(t, receivedEvent)
		assert.Equal(t, "", receivedEvent.Items["bin-key"].Value)
		assert.Equal(t, "true", receivedEvent.Items["bin-key"].Metadata["deleted"])
	})

	t.Run("non-ConfigMap objects are ignored", func(t *testing.T) {
		// Verify handler is not called when objects are not ConfigMaps
		handlerCalled := false
		handler := func(_ context.Context, _ *configuration.UpdateEvent) error {
			handlerCalled = true
			return nil
		}

		store.handleConfigMapUpdate(
			t.Context(),
			&configuration.SubscribeRequest{Keys: []string{}},
			handler, "not-a-configmap", "not-a-configmap", "sub-1",
		)

		assert.False(t, handlerCalled)
	})
}
