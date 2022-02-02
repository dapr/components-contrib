/*
Copyright 2021 The Dapr Authors
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

package firestore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
)

func TestGetFirestoreMetadata(t *testing.T) {
	t.Run("With correct properties", func(t *testing.T) {
		properties := map[string]string{
			"type":                        "service_account",
			"project_id":                  "myprojectid",
			"private_key_id":              "123",
			"private_key":                 "mykey",
			"client_email":                "me@123.iam.gserviceaccount.com",
			"client_id":                   "456",
			"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
			"token_uri":                   "https://oauth2.googleapis.com/token",
			"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
			"client_x509_cert_url":        "https://www.googleapis.com/robot/v1/metadata/x509/x",
		}
		m := state.Metadata{
			Properties: properties,
		}
		metadata, err := getFirestoreMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "service_account", metadata.Type)
		assert.Equal(t, "myprojectid", metadata.ProjectID)
		assert.Equal(t, "123", metadata.PrivateKeyID)
		assert.Equal(t, "mykey", metadata.PrivateKey)
		assert.Equal(t, defaultEntityKind, metadata.EntityKind)
	})

	t.Run("With incorrect properties", func(t *testing.T) {
		properties := map[string]string{
			"type":           "service_account",
			"project_id":     "myprojectid",
			"private_key_id": "123",
			"private_key":    "mykey",
		}
		m := state.Metadata{
			Properties: properties,
		}
		_, err := getFirestoreMetadata(m)
		assert.NotNil(t, err)
	})
}
