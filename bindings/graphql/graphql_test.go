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

package graphql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestOperations(t *testing.T) {
	t.Parallel()
	t.Run("Get operation list", func(t *testing.T) {
		t.Parallel()
		b := NewGraphQL(nil)
		assert.NotNil(t, b)
		l := b.Operations()
		assert.Equal(t, 2, len(l))
	})
}

func InitBinding(s *httptest.Server, extraProps map[string]string) (bindings.OutputBinding, error) {
	m := bindings.Metadata{Base: metadata.Base{
		Properties: map[string]string{
			"endpoint": s.URL,
		},
	}}

	for k, v := range extraProps {
		m.Properties[k] = v
	}

	binding := NewGraphQL(logger.NewLogger("test"))
	err := binding.Init(context.Background(), m)
	return binding, err
}

func TestInit(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer s.Close()

	_, err := InitBinding(s, nil)
	require.NoError(t, err)
}

type TestGraphQLRequest struct {
	Query     string            `json:"query"`
	Variables map[string]string `json:"variables"`
}

func TestGraphQlRequestHeadersAndVariables(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)

		testHeader := r.Header.Get("X-Test-Header")
		require.Equal(t, "test-header-value", testHeader)

		var rBody *TestGraphQLRequest
		json.NewDecoder(r.Body).Decode(&rBody)

		require.Contains(t, rBody.Variables, "episode")
		require.Equal(t, "JEDI", rBody.Variables["episode"])

		m := make(map[string]interface{})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(m)
	}))
	defer s.Close()

	gql, err := InitBinding(s, nil)
	require.NoError(t, err)

	req := &bindings.InvokeRequest{
		Operation: "query",
		Metadata: map[string]string{
			"query": `query HeroNameAndFriends($episode: Episode) {
				hero(episode: $episode) {
				  name
				  friends {
					name
				  }
				}
			  }`,
			"header:X-Test-Header": "test-header-value",
			"variable:episode":     "JEDI",
		},
	}
	_, err = gql.Invoke(context.Background(), req)
	require.NoError(t, err)
}
