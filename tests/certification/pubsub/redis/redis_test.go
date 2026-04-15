package redis_test

import (
	"context"
	"os/exec"
	"strconv"
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
	grpcPort := 50002
	httpPort := 3501

	flow.New(t, "Redis Sentinel Failover Test").
		// 1. Start the Sentinel Cluster
		Step(dockercompose.Run("redis-sentinel-test", "components/sentinel/docker-compose.yml")).
		Step("Wait for Redis to be ready", flow.Sleep(15*time.Second)).

		// 2. Start the Dapr Embedded Runtime using sidecar
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/sentinel"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		// 3. Wait for the sidecar to be ready
		Step("Wait for sidecar to be ready", flow.Sleep(15*time.Second)).

		// 4. Perform Initial Publish
		Step("Initial Publish", func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName)
			err := client.PublishEvent(context.Background(), pubsubName, testTopic, []byte("initial message"))
			require.NoError(t, err, "Initial publish should succeed")
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
		Step("Wait for failover to complete", flow.Sleep(15*time.Second)).

		// 7. Perform Second Publish
		Step("Second Publish After Failover", func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName)
			err := client.PublishEvent(context.Background(), pubsubName, testTopic, []byte("second message"))

			// This assertion now expects success because components-contrib mitigates the EOF error
			require.NoError(t, err, "Expected no error due to Dapr automatically applying connection pool recycling for Sentinel")

			return nil
		}).
		Run()
}
