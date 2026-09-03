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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/aws/s3"
	"github.com/dapr/components-contrib/binarystore/azure/blobstorage"
	"github.com/dapr/components-contrib/binarystore/azure/datalake"
	gcp_bucket "github.com/dapr/components-contrib/binarystore/gcp/bucket"
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
			if shouldSkipBinaryStoreComponent(t, comp.Component) {
				return
			}

			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			store := loadBinaryStoreComponent(comp.Component)
			require.NotNil(t, store, "error running conformance test for component %s", comp.Component)

			conf_binarystore.ConformanceTests(t, props, store, comp.Component)
		}
	}

	tc.Run(t)
}

// shouldSkipBinaryStoreComponent skips tests whose required environment
// variables are not set.
func shouldSkipBinaryStoreComponent(t *testing.T, componentName string) bool {
	switch componentName {
	case "azure.blobstorage":
		if os.Getenv("AzureBlobStorageAccount") == "" || os.Getenv("AzureBlobStorageAccessKey") == "" {
			t.Skipf("Skipping Azure Blob Storage conformance test: AzureBlobStorageAccount and AzureBlobStorageAccessKey environment variables must be set")
			return true
		}
	case "azure.datalake":
		if os.Getenv("AzureBlobStorageAccount") == "" || os.Getenv("AzureBlobStorageAccessKey") == "" {
			t.Skipf("Skipping Azure Data Lake Storage conformance test: AzureBlobStorageAccount and AzureBlobStorageAccessKey environment variables must be set")
			return true
		}
	case "aws.s3":
		if os.Getenv("AWSS3Bucket") == "" {
			t.Skipf("Skipping AWS S3 conformance test: AWSS3Bucket environment variable must be set")
			return true
		}
	case "gcp.bucket":
		if os.Getenv("GCPBucket") == "" {
			t.Skipf("Skipping Google Cloud Storage conformance test: GCPBucket environment variable must be set")
			return true
		}
	case "oci.objectstorage":
		if os.Getenv("DAPR_TEST_OCI_CONFIG_FILE_PATH") == "" ||
			os.Getenv("DAPR_TEST_OCI_COMPARTMENT_OCID") == "" ||
			os.Getenv("DAPR_TEST_OCI_BUCKET_NAME") == "" {
			t.Skipf("Skipping OCI Object Storage conformance test: DAPR_TEST_OCI_CONFIG_FILE_PATH, DAPR_TEST_OCI_COMPARTMENT_OCID, and DAPR_TEST_OCI_BUCKET_NAME environment variables must be set")
			return true
		}
	}
	return false
}

func loadBinaryStoreComponent(name string) binarystore.BinaryStore {
	switch name {
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
