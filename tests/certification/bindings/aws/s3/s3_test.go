/*
Copyright 2023 The Dapr Authors
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

package awss3binding_test

import (
	"testing"

	bindings_s3 "github.com/dapr/components-contrib/bindings/aws/s3"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/kit/logger"

	"github.com/dapr/dapr/pkg/runtime"
)

func init() {

}

func TestAWSS3CertificationTests(t *testing.T) {
	defer teardown(t)

	t.Run("S3SBasic", func(t *testing.T) {
		S3SBasic(t)
	})
}

// Verify S3 Create Object
func S3SBasic(t *testing.T) {

}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(bindings_s3.NewAWSS3, "aws.s3")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}

func teardown(t *testing.T) {
	t.Logf("AWS S3 Binding CertificationTests teardown...")
	//Dapr runtime automatically creates the following queues, topics
	//so here they get deleted.

	t.Logf("AWS S3 Binding CertificationTests teardown...done!")
}
