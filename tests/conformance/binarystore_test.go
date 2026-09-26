//go:build conftests
// +build conftests

/*
Copyright 2025 The Dapr Authors
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

package conformance

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/aws/s3"
	"github.com/dapr/components-contrib/binarystore/azure/blobstorage"
	"github.com/dapr/components-contrib/binarystore/azure/datalake"
	gcp_bucket "github.com/dapr/components-contrib/binarystore/gcp/bucket"
	inmemory "github.com/dapr/components-contrib/binarystore/in-memory"
	oci_objectstorage "github.com/dapr/components-contrib/binarystore/oci/objectstorage"
	conf_binarystore "github.com/dapr/components-contrib/tests/conformance/binarystore"
	"github.com/dapr/components-contrib/tests/conformance/utils"
)

func TestBinaryStoreConformance(t *testing.T) {
	const configPath = "../config/binarystore/"

	// Try to load environment variables from .env file
	utils.LoadEnvVars(configPath + ".env")

	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			componentConfigPath := filepath.Join(configPath, convertComponentNameToPath(comp.Component, comp.Profile))

			if skipIfEnvVarsMissing(t, comp.Component, componentConfigPath) {
				return
			}

			ParseConfigurationMap(t, comp.Config)

			props, err := loadComponentsAndProperties(t, componentConfigPath)
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			store := loadBinaryStoreComponent(comp.Component)
			require.NotNil(t, store, "error running conformance test for component %s", comp.Component)

			conf_binarystore.ConformanceTests(t, props, store, comp.Component)
		}
	}

	tc.Run(t)
}

// skipIfEnvVarsMissing skips the test if any environment variable interpolated
// by the component's YAML definition is unset. The list of required variables is
// derived from the YAML itself so it can never drift from the component configs.
func skipIfEnvVarsMissing(t *testing.T, componentName, componentConfigPath string) bool {
	missing := missingEnvVarsForComponent(t, componentConfigPath)
	if len(missing) == 0 {
		return false
	}

	t.Skipf("Skipping %s conformance test: the following environment variables must be set: %s", componentName, strings.Join(missing, ", "))
	return true
}

// missingEnvVarsForComponent returns the sorted, de-duplicated names of the
// environment variables referenced as ${{VAR}} by the component definitions at
// componentConfigPath which are unset or empty.
func missingEnvVarsForComponent(t *testing.T, componentConfigPath string) []string {
	comps, err := LoadComponents(componentConfigPath)
	require.NoErrorf(t, err, "error loading components from %s", componentConfigPath)

	missing := make(map[string]struct{})
	for _, c := range comps {
		for _, item := range c.Spec.Metadata {
			name, ok := envVarReference(item.Value.String())
			if !ok {
				continue
			}
			if LookUpEnv(name) == "" {
				missing[name] = struct{}{}
			}
		}
	}

	names := make([]string, 0, len(missing))
	for name := range missing {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// envVarReference extracts the environment variable name from a ${{VAR}}
// metadata value, matching the interpolation performed by parseMetadataProperty.
func envVarReference(val string) (string, bool) {
	if !strings.HasPrefix(val, "${{") || !strings.HasSuffix(val, "}}") {
		return "", false
	}

	return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(val, "${{"), "}}")), true
}

func loadBinaryStoreComponent(name string) binarystore.BinaryStore {
	switch name {
	case "in-memory":
		return inmemory.NewInMemoryBinaryStore(testLogger)
	case "azure.blobstorage":
		return blobstorage.NewAzureBlobStorage(testLogger)
	case "azure.datalake":
		return datalake.NewAzureDataLakeStorage(testLogger)
	case "aws.s3":
		return s3.NewAWSS3(testLogger)
	case "gcp.bucket":
		return gcp_bucket.NewGCPBucket(testLogger)
	case "oci.objectstorage":
		return oci_objectstorage.NewOCIObjectStorage(testLogger)
	default:
		return nil
	}
}
