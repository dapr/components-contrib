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

package git_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	config_git "github.com/dapr/components-contrib/configuration/git"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
	cu_git "github.com/dapr/components-contrib/tests/utils/configupdater/git"
	configuration_loader "github.com/dapr/dapr/pkg/components/configuration"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	storeName    = "configstore"
	key1         = "key1"
	key2         = "key2"
	val1         = "val1"
	val2         = "val2"
	sidecarName1 = "dapr-1"
	pollInterval = "250ms"
)

// castConfigurationItems converts go-sdk ConfigurationItem to contrib ConfigurationItem.
func castConfigurationItems(items map[string]*dapr.ConfigurationItem) map[string]*configuration.Item {
	configItems := make(map[string]*configuration.Item)
	for key, item := range items {
		configItems[key] = &configuration.Item{
			Value:    item.Value,
			Version:  item.Version,
			Metadata: item.Metadata,
		}
	}
	return configItems
}

// seedUpstream initialises a bare git upstream and returns its file:// URL.
// Cleanup is registered with t.Cleanup. The upstream starts with a single
// empty commit on `main` so the store-under-test can clone successfully.
func seedUpstream(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", "dapr-cert-git-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	bare := filepath.Join(root, "upstream.git")
	seed := filepath.Join(root, "seed")

	mustRun := func(dir, name string, args ...string) {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "%s %v: %s", name, args, out)
	}

	mustRun("", "git", "init", "--bare", bare)
	mustRun("", "git", "clone", bare, seed)
	mustRun(seed, "git", "config", "user.email", "cert@dapr.io")
	mustRun(seed, "git", "config", "user.name", "cert")
	mustRun(seed, "git", "config", "commit.gpgsign", "false")
	mustRun(seed, "git", "commit", "--allow-empty", "-m", "initial")
	mustRun(seed, "git", "branch", "-M", "main")
	mustRun(seed, "git", "push", "origin", "main")
	// Point the bare repo's HEAD at refs/heads/main so go-git's PlainClone
	// in the updater can resolve a meaningful default branch instead of
	// falling into the no-refs path.
	mustRun("", "git", "--git-dir", bare, "symbolic-ref", "HEAD", "refs/heads/main")

	return fileURL(bare)
}

// fileURL produces a portable file:// URL for an absolute local path.
// On Unix the path already starts with `/`, so `file://` + path yields the
// canonical three-slash form. On Windows the path starts with a drive
// letter (e.g. `C:\foo`), so we normalise separators and prepend `/`
// to produce the standard `file:///C:/foo` form.
func fileURL(absPath string) string {
	p := filepath.ToSlash(absPath)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return "file://" + p
}

// writeComponentSpec writes the Dapr component spec for the configuration
// store under test to a fresh temp directory and returns its path. The
// upstream URL is injected directly because Dapr does not interpolate
// environment variables in component metadata values. Using a temp dir
// avoids overwriting the placeholder spec checked into components/default.
func writeComponentSpec(t *testing.T, upstreamURL string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dapr-cert-git-components-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	spec := fmt.Sprintf(`apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: configstore
spec:
  type: configuration.git
  version: v1
  metadata:
    - name: url
      value: %q
    - name: branch
      value: "main"
    - name: path
      value: "."
    - name: mappingMode
      value: "file"
    - name: pollInterval
      value: %q
    - name: emitInitialState
      value: "false"
    - name: authMode
      value: "none"
`, upstreamURL, pollInterval)
	path := filepath.Join(dir, "configstore.yaml")
	require.NoError(t, os.WriteFile(path, []byte(spec), 0o644))
	return dir
}

func TestGit(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	upstreamURL := seedUpstream(t)
	componentsPath := writeComponentSpec(t, upstreamURL)

	configurationRegistry := configuration_loader.NewRegistry()
	configurationRegistry.Logger = log
	configurationRegistry.RegisterComponent(config_git.NewGitConfigurationStore, "git")

	updater := cu_git.NewGitConfigUpdater(log).(*cu_git.ConfigUpdater)

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	messageWatcher := watcher.NewUnordered()
	var subscribeIDs []string

	initUpdater := func(_ flow.Context) error {
		return updater.Init(map[string]string{
			"url":          upstreamURL,
			"branch":       "main",
			"pollInterval": pollInterval,
		})
	}

	checkConnection := func(_ flow.Context) error {
		probe := map[string]*configuration.Item{"healthcheck": {Value: "ok"}}
		if err := updater.AddKey(probe); err != nil {
			return err
		}
		return updater.DeleteKey([]string{"healthcheck"})
	}

	testGet := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)

		require.NoError(ctx.T, updater.AddKey(map[string]*configuration.Item{
			key1: {Value: val1},
			key2: {Value: val2},
		}))

		// Wait until the sidecar's snapshot reflects the writes — uses
		// require.Eventually rather than a fixed-interval sleep loop.
		var items map[string]*dapr.ConfigurationItem
		require.Eventually(ctx.T, func() bool {
			items, err = client.GetConfigurationItems(ctx, storeName, []string{key1})
			return err == nil && len(items) == 1
		}, 15*time.Second, 200*time.Millisecond, "snapshot did not reflect write within deadline")
		require.NoError(ctx.T, err)
		require.Len(ctx.T, items, 1)
		require.Equal(ctx.T, val1, items[key1].Value)

		items, err = client.GetConfigurationItems(ctx, storeName, []string{key1, key2})
		require.NoError(ctx.T, err)
		require.Len(ctx.T, items, 2)
		require.Equal(ctx.T, val1, items[key1].Value)
		require.Equal(ctx.T, val2, items[key2].Value)

		items, err = client.GetConfigurationItems(ctx, storeName, []string{"nonexistent"})
		require.NoError(ctx.T, err)
		require.Empty(ctx.T, items)
		return nil
	}

	subscribeFn := func(keys []string, message *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			message.Reset()
			subID, errSubscribe := client.SubscribeConfigurationItems(ctx, storeName, keys,
				func(_ string, items map[string]*dapr.ConfigurationItem) {
					evt := &configuration.UpdateEvent{Items: castConfigurationItems(items)}
					// Strip system-assigned version (commit SHA) and normalise nil
					// metadata for stable JSON comparison.
					for _, item := range evt.Items {
						item.Version = ""
						if item.Metadata == nil {
							item.Metadata = map[string]string{}
						}
					}
					payload, err := json.Marshal(evt)
					if err != nil {
						log.Errorf("failed to marshal subscription event: %v", err)
						return
					}
					message.Observe(string(payload))
				})
			subscribeIDs = append(subscribeIDs, subID)
			return errSubscribe
		}
	}

	verifySubscriberReady := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()
			probe := configuration.UpdateEvent{
				Items: map[string]*configuration.Item{
					key1: {Value: "readiness-probe", Metadata: map[string]string{}},
				},
			}
			payload, err := json.Marshal(probe)
			if err != nil {
				return fmt.Errorf("marshal probe: %w", err)
			}
			messages.Expect(string(payload))
			if err := updater.UpdateKey(map[string]*configuration.Item{
				key1: {Value: "readiness-probe"},
			}); err != nil {
				return fmt.Errorf("update probe: %w", err)
			}
			messages.Assert(ctx.T, 30*time.Second)
			return nil
		}
	}

	testSubscribe := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()
			expected := configuration.UpdateEvent{
				Items: map[string]*configuration.Item{
					key1: {Value: "updated-val1", Metadata: map[string]string{}},
				},
			}
			payload, err := json.Marshal(expected)
			if err != nil {
				return fmt.Errorf("marshal expected update: %w", err)
			}
			messages.Expect(string(payload))
			require.NoError(ctx.T, updater.UpdateKey(map[string]*configuration.Item{
				key1: {Value: "updated-val1"},
			}))
			messages.Assert(ctx.T, 30*time.Second)
			return nil
		}
	}

	testDelete := func(messages *watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			messages.Reset()
			expected := configuration.UpdateEvent{
				Items: map[string]*configuration.Item{
					key2: {Value: "", Metadata: map[string]string{"deleted": "true"}},
				},
			}
			payload, err := json.Marshal(expected)
			if err != nil {
				return fmt.Errorf("marshal expected delete: %w", err)
			}
			messages.Expect(string(payload))
			require.NoError(ctx.T, updater.DeleteKey([]string{key2}))
			messages.Assert(ctx.T, 30*time.Second)
			return nil
		}
	}

	stopSubscribers := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)
		for _, subID := range subscribeIDs {
			if err := client.UnsubscribeConfigurationItems(ctx, storeName, subID); err != nil {
				return err
			}
		}
		subscribeIDs = nil
		return nil
	}

	testGetAfterUpdate := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarName1)
		items, err := client.GetConfigurationItems(ctx, storeName, []string{key1})
		require.NoError(ctx.T, err)
		require.Len(ctx.T, items, 1)
		require.Equal(ctx.T, "updated-val1", items[key1].Value)
		return nil
	}

	flow.New(t, "git configuration certification test").
		Step("initialize updater", initUpdater).
		Step("verify connection", checkConnection).
		Step(sidecar.Run(sidecarName1,
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithResourcesPath(componentsPath),
			embedded.WithConfigurations(configurationRegistry),
		)).
		Step("test get", testGet).
		Step("start subscriber", subscribeFn([]string{key1, key2}, messageWatcher)).
		Step("verify subscriber ready", verifySubscriberReady(messageWatcher)).
		Step("reset", flow.Reset(messageWatcher)).
		Step("test subscribe updates", testSubscribe(messageWatcher)).
		Step("reset", flow.Reset(messageWatcher)).
		Step("test delete notifications", testDelete(messageWatcher)).
		Step("reset", flow.Reset(messageWatcher)).
		Step("stop subscribers", stopSubscribers).
		Step("verify data after unsubscribe", testGetAfterUpdate).
		Run()
}
