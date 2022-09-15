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
package env

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
)

func TestInit(t *testing.T) {
	secret := "secret1"
	key := "TEST_SECRET"

	s := envSecretStore{logger: logger.NewLogger("test")}

	os.Setenv(key, secret)
	assert.Equal(t, secret, os.Getenv(key))

	t.Run("Test init", func(t *testing.T) {
		err := s.Init(secretstores.Metadata{})
		assert.Nil(t, err)
	})

	t.Run("Test set and get", func(t *testing.T) {
		err := s.Init(secretstores.Metadata{})
		assert.Nil(t, err)
		resp, err := s.GetSecret(context.Background(), secretstores.GetSecretRequest{Name: key})
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, secret, resp.Data[key])
	})

	t.Run("Test bulk get", func(t *testing.T) {
		err := s.Init(secretstores.Metadata{})
		assert.Nil(t, err)
		resp, err := s.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, secret, resp.Data[key][key])
	})
}

func TestGetFeatures(t *testing.T) {
	s := envSecretStore{logger: logger.NewLogger("test")}
	// Yes, we are skipping initialization as feature retrieval doesn't depend on it.
	t.Run("no features are advertised", func(t *testing.T) {
		f := s.Features()
		assert.Empty(t, f)
	})
}
