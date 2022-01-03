package objectstorage

// for example in ~/dapr-dev/components-contrib
// go test -v github.com/dapr/components-contrib/state/oci/objectstorage
// to run the test set for OCI ObjectStorage service based StateStore.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

var ociObjectStorageConfiguration = map[string]string{
	"bucketName":      "myBuck",
	"tenancyOCID":     "ocid1.tenancy.oc1..aaaaaaaag7c7sq",
	"userOCID":        "ocid1.user.oc1..aaaaaaaaby",
	"compartmentOCID": "ocid1.compartment.oc1..aaaaaaaq",
	"fingerPrint":     "02:91:6c",
	"privateKey":      "-----BEGIN RSA PRIVATE KEY-----\nMIIEogI=\n-----END RSA PRIVATE KEY-----",
	"region":          "us-ashburn-1",
}

func TestInit(t *testing.T) {
	meta := state.Metadata{}
	statestore := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Init with missing metadata", func(t *testing.T) {
		meta.Properties = map[string]string{
			"invalidValue": "a",
		}
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("missing or empty bucketName field from metadata"), err, "Lacking configuration property should be spotted")
	})

	t.Run("Init with incorrect metadata", func(t *testing.T) {
		meta.Properties = ociObjectStorageConfiguration
		err := statestore.Init(meta)
		assert.NotNil(t, err)
		assert.Error(t, err, "Incorrect configuration data should result in failure to create client")
		assert.Contains(t, err.Error(), "failed to initialize client", "Incorrect configuration data should result in failure to create client")
	})
}

func TestFeatures(t *testing.T) {
	s := NewOCIObjectStorageStore(logger.NewLogger("logger"))
	t.Run("Test contents of Features", func(t *testing.T) {
		features := s.Features()
		assert.Contains(t, features, state.FeatureETag)
	})
}
