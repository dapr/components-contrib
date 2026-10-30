/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    10|See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/pkg/config/protocol"
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	appIDDlqErr       = "app-dlq-err"
	sidecarNameDlqErr = "dapr-dlq-err"
	dlqErrTopic       = "orders-dlq-err"
	dlqErrRawTopic    = "orders-dlq-err-raw"
	dlqErrPayload     = `{"orderId":"42"}`
	dlqErrMessage     = "bad payload"
	dlqErrRawMessage  = "raw processing failed"
)

// TestKafkaDlqErrorDetails covers dapr/dapr#8519 / dapr/dapr#11003.
//
// When Pubsub.DeadLetterErrorDetails is enabled and the subscription opts in
// with deadLetterErrorDetails=true, an app DROP response that includes a
// message must surface on the dead-letter publish:
//   - CloudEvent path: errordetails CE extension
//   - rawPayload path: __error publish metadata (Kafka header → HTTP header)
func TestKafkaDlqErrorDetails(t *testing.T) {
	// Skipped until the runtime feature ships in the pinned daprd.
	//
	// Enrichment lives in github.com/dapr/dapr (Pubsub.DeadLetterErrorDetails
	// preview feature + subscription metadata). The revision pinned in
	// tests/certification/go.mod does not include it yet, so the DLQ event
	// would arrive without details and the assertions below would fail.
	//
	// To re-enable: bump github.com/dapr/dapr in tests/certification/go.mod to
	// a revision carrying dapr/dapr#11003, then delete this skip. For a local
	// check beforehand, temporarily add:
	//   replace github.com/dapr/dapr => ../../../dapr
	// to tests/certification/go.mod (do not commit the replace).
	t.Skip("needs daprd with Pubsub.DeadLetterErrorDetails; bump tests/certification/go.mod after dapr/dapr#11003")

	var (
		mu               sync.Mutex
		gotCEDetails     string
		gotRawDetails    string
		ceDlqDeliveries  int
		rawDlqDeliveries int
	)

	dlqErrAppPort := appPort + portOffset*5

	dropWithMessage := func(message string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"status":"DROP","message":%q}`, message)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/orders", dropWithMessage(dlqErrMessage))
	mux.HandleFunc("/orders-raw", dropWithMessage(dlqErrRawMessage))
	mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var envelope map[string]any
		if err := json.Unmarshal(body, &envelope); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		ceDlqDeliveries++
		if v, ok := envelope["errordetails"].(string); ok {
			gotCEDetails = v
		}
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"SUCCESS"}`))
	})
	mux.HandleFunc("/dlq-raw", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)

		mu.Lock()
		rawDlqDeliveries++
		// Kafka stores PublishRequest.Metadata as headers; daprd forwards them
		// to the app as HTTP headers (case-insensitive).
		gotRawDetails = r.Header.Get("__error")
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"SUCCESS"}`))
	})

	var srv *http.Server
	startApp := func(ctx flow.Context) error {
		srv = &http.Server{
			Addr:    fmt.Sprintf(":%d", dlqErrAppPort),
			Handler: mux,
		}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				ctx.Logf("dlq error-details app listen error: %v", err)
			}
		}()
		return nil
	}
	stopApp := func(ctx flow.Context) error {
		if srv == nil {
			return nil
		}
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}

	sidecarOptions := append(componentRuntimeOptions(),
		embedded.WithResourcesPath("./components/consumerDlqErrorDetails"),
		embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(dlqErrAppPort)),
		embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort+portOffset*5)),
		embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort+portOffset*5)),
		embedded.WithProfilePort(strconv.Itoa(runtime.DefaultProfilePort+portOffset*5)),
	)

	publishCE := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarNameDlqErr)
		err := client.PublishEvent(ctx, pubsubName, dlqErrTopic, dlqErrPayload,
			dapr.PublishEventWithMetadata(map[string]string{messageKey: "dlq-err"}))
		require.NoError(ctx, err, "error publishing CloudEvent message")
		return nil
	}

	publishRaw := func(ctx flow.Context) error {
		client := sidecar.GetClient(ctx, sidecarNameDlqErr)
		err := client.PublishEvent(ctx, pubsubName, dlqErrRawTopic, dlqErrPayload,
			dapr.PublishEventWithMetadata(map[string]string{
				messageKey:   "dlq-err-raw",
				"rawPayload": "true",
			}))
		require.NoError(ctx, err, "error publishing rawPayload message")
		return nil
	}

	assertCEErrorDetails := func(ctx flow.Context) error {
		require.Eventually(ctx, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return ceDlqDeliveries > 0 && gotCEDetails == dlqErrMessage
		}, time.Minute, 100*time.Millisecond,
			"DLQ subscriber should receive a CloudEvent with errordetails=%q", dlqErrMessage)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(ctx, dlqErrMessage, gotCEDetails)
		require.GreaterOrEqual(ctx, ceDlqDeliveries, 1)
		return nil
	}

	assertRawErrorDetails := func(ctx flow.Context) error {
		require.Eventually(ctx, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return rawDlqDeliveries > 0 && gotRawDetails == dlqErrRawMessage
		}, time.Minute, 100*time.Millisecond,
			"raw DLQ subscriber should receive HTTP header __error=%q", dlqErrRawMessage)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(ctx, dlqErrRawMessage, gotRawDetails)
		require.GreaterOrEqual(ctx, rawDlqDeliveries, 1)
		return nil
	}

	flow.New(t, "kafka dead-letter error details").
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for broker sockets",
			network.WaitForAddresses(5*time.Minute, brokers...)).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("wait for kafka readiness", retry.Do(10*time.Second, 30, func(ctx flow.Context) error {
			config := sarama.NewConfig()
			config.ClientID = "test-consumer-dlq-err"
			config.Consumer.Return.Errors = true

			client, err := sarama.NewConsumer(brokers, config)
			if err != nil {
				return err
			}
			defer client.Close()

			_, err = client.ConsumePartition("myTopic", 0, sarama.OffsetOldest)
			return err
		})).
		Step("start "+appIDDlqErr, startApp).
		Step(sidecar.Run(sidecarNameDlqErr, sidecarOptions...)).
		Step("publish CloudEvent message the app drops with details", publishCE).
		Step("assert DLQ CloudEvent carries errordetails", assertCEErrorDetails).
		Step("publish rawPayload message the app drops with details", publishRaw).
		Step("assert raw DLQ delivery carries __error header", assertRawErrorDetails).
		Step("stop sidecar", sidecar.Stop(sidecarNameDlqErr)).
		Step("stop "+appIDDlqErr, stopApp).
		Run()
}
