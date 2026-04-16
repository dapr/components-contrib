package redis_test

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	redispubsub "github.com/dapr/components-contrib/pubsub/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	pubsubLoader "github.com/dapr/dapr/pkg/components/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/require"
)

const (
	pubsubName  = "pubsub-redis-sentinel"
	testTopic   = "test-topic-failover"
	sidecarName = "redis-sentinel-sidecar"
)

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")
	registry := pubsubLoader.NewRegistry()
	registry.Logger = log
	registry.RegisterComponent(func(l logger.Logger) pubsub.PubSub {
		return redispubsub.NewRedisStreams(l)
	}, "redis")

	return []embedded.Option{
		embedded.WithPubSubs(registry),
	}
}

func TestRedisSentinelFailover(t *testing.T) {
	// We run the test twice to prove the fix: once with default settings (expected to fail)
	// and once with explicitly set timeouts (expected to pass).
	testCases := []struct {
		name        string
		configPath  string
		expectError bool
	}{
		{
			name:        "Fails With Default Timeouts",
			configPath:  "components/sentinel/fail_test",
			expectError: true,
		},
		{
			name:        "Passes With 10s Timeouts",
			configPath:  "components/sentinel/pass_test",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sidecarName := fmt.Sprintf("redis-sentinel-sidecar-%s", tc.name)

			flow.New(t, tc.name).
				// 1. Start the Sentinel Cluster
				Step(dockercompose.Run("redis-sentinel-test", "components/sentinel/docker-compose.yml")).
				Step("Wait for Redis to be ready", flow.Sleep(15*time.Second)).

				// 2. Start the Dapr sidecar
				Step(sidecar.Run(sidecarName,
					append(componentRuntimeOptions(),
						embedded.WithoutApp(),
						// Use the specific config file for this test case
						embedded.WithResourcesPath(tc.configPath),
						embedded.WithDaprGRPCPort(strconv.Itoa(50002)),
						embedded.WithDaprHTTPPort(strconv.Itoa(3501)),
					)...,
				)).

				// 3. Wait for the sidecar to be ready
				Step("Wait for sidecar to be ready", flow.Sleep(15*time.Second)).

				// 4. Perform Initial Publish
				Step("Initial Publish", func(ctx flow.Context) error {
					client := sidecar.GetClient(ctx, sidecarName)
					// PUBLISH 20 TIMES CONCURRENTLY TO FILL THE POOL
					var wg sync.WaitGroup
					for i := 0; i < 20; i++ {
						wg.Add(1)
						go func(idx int) {
							defer wg.Done()
							_ = client.PublishEvent(context.Background(), pubsubName, testTopic, []byte(fmt.Sprintf("initial message %d", idx)))
						}(i)
					}
					wg.Wait()
					return nil
				}).

				// 5. Trigger Sentinel Failover
				Step("Trigger Failover", func(ctx flow.Context) error {
					// Find the sentinel container ID and trigger failover
					// Using standard docker exec command as a simple way to invoke the failover
					out, err := exec.Command("docker", "exec", "redis-sentinel-test-redis-cluster-1", "redis-cli", "-p", "26379", "SENTINEL", "failover", "mymaster").CombinedOutput()
					ctx.Log(string(out))
					return err
				}).

				// 6. Wait for idle connection and failover completion
				Step("Wait for failover to complete", flow.Sleep(5*time.Second)).

				// 6.5 KILL THE OLD MASTER TO FORCE TCP FIN/RST (Simulate Proxy Drop)
				Step("Kill old master to force socket closure", func(ctx flow.Context) error {
					// We kill the redis-server process on port 6379 (the original master) to forcefully close the socket Dapr holds
					// Using kill -9 on the specific PID bound to 6379 to avoid killing the Sentinel process
					out, _ := exec.Command("docker", "exec", "redis-sentinel-test-redis-cluster-1", "sh", "-c", "kill -9 $(lsof -t -i:6379)").CombinedOutput()
					ctx.Log("Kill old master output: " + string(out))
					return nil
				}).
				Step("Wait a bit more", flow.Sleep(10*time.Second)).

				// 7. Perform Second Publish
				Step("Second Publish After Failover", func(ctx flow.Context) error {
					client := sidecar.GetClient(ctx, sidecarName)
					err := client.PublishEvent(context.Background(), pubsubName, testTopic, []byte("second message"))

					if tc.expectError {
						require.Error(ctx.T, err, "Expected an error because we are testing the failing condition without pool recycling")
						if err != nil {
							// The OS either returns EOF (if graceful close) or connection reset (if abrupt kill)
							require.True(ctx.T, strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection reset"), "Expected EOF or connection reset error, got: "+err.Error())
						}
					} else {
						require.NoError(ctx.T, err, "Expected no error. Dapr should gracefully dial a new connection after failover.")
					}

					return nil
				}).
				Run()
		})
	}
}
