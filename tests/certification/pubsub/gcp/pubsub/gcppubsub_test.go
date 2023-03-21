/*
Copyright 2022 The Dapr Authors
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

package pubsub_test

import (
	"context"
	"fmt"
	"sync/atomic"

	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	pubsub_gcppubsub "github.com/dapr/components-contrib/pubsub/gcp/pubsub"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages      = 10
	appPort          = 8000
	portOffset       = 2
	pubsubName       = "gcp-pubsub-cert-tests"
	topicActiveName  = "certification-pubsub-topic-active"
	topicPassiveName = "certification-pubsub-topic-passive"
	topicToBeCreated = "certification-topic-per-test-run"
	topicDefaultName = "certification-topic-default"
)

func init() {
	// qn := os.Getenv("GCP_REGION")
	// if qn != "" {
	// 	region = qn
	// }

	// qn = os.Getenv("GCP_PROJECT_ID")
	// if qn != "" {
	// 	topics = append(topics, qn)
	// }
}

func TestGCPPubSubCertificationTests(t *testing.T) {
	defer teardown(t)

	t.Run("GCPPubSubBasic", func(t *testing.T) {
		GCPPubSubBasic(t)
	})
}

// Verify with single publisher / single subscriber
func GCPPubSubBasic(t *testing.T) {

}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	pubsubRegistry := pubsub_loader.NewRegistry()
	pubsubRegistry.Logger = log
	pubsubRegistry.RegisterComponent(pubsub_gcppubsub.NewGCPPubSub, "gcp.pubsub")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithPubSubs(pubsubRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}

func teardown(t *testing.T) {
	t.Logf("GCP PubSub CertificationTests teardown...")
	//Dapr runtime automatically creates the following subscriptions, topics
	//so here they get deleted.
	// if err := deleteSubscriptions(subscriptions); err != nil {
	// 	t.Log(err)
	// }

	// if err := deleteTopics(topics); err != nil {
	// 	t.Log(err)
	// }

	t.Logf("GCP PubSub CertificationTests teardown...done!")
}
