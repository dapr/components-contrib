// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kubernetes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/kubernetes-client/go/kubernetes/client"
	"github.com/stretchr/testify/assert"
)

func makerFn() interface{} { return &client.V1Namespace{} }

type staticHandler struct {
	Code        int
	Body        string
	QueryParams url.Values
}

func (s *staticHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(s.Code)
	res.Write([]byte(s.Body))
	s.QueryParams = req.URL.Query()
}

func TestParseMetadata(t *testing.T) {
	nsName := "fooNamespace"
	m := bindings.Metadata{}
	m.Properties = map[string]string{"namespace": nsName}

	i := kubernetesInput{}
	i.parseMetadata(m)

	assert.Equal(t, nsName, i.namespace, "The namespaces should be the same.")
}

func TestReadItem(t *testing.T) {
	server := httptest.NewServer(&staticHandler{
		Code: 200,
		Body: `{"type":"ADDED","object":{"kind":"Event","apiVersion":"v1","metadata":{"name":"kube-system","selfLink":"/api/v1/namespaces/default/test","uid":"164931a7-3d75-11e9-a0a0-2683b9459238","resourceVersion":"227","creationTimestamp":"2019-03-03T05:27:50Z","annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{\"annotations\":{},\"name\":\"kube-system\",\"namespace\":\"\"}}\n"}},"spec":{"finalizers":["kubernetes"]},"status":{"phase":"Active"}}}\n`,
	})
	defer server.Close()

	u, err := url.Parse(server.URL)
	if !assert.NoError(t, err, "URL Parsing failed!") {
		t.FailNow()
	}

	cfg := &client.Configuration{}
	cfg.Host = u.Host
	cfg.Scheme = u.Scheme

	i := &kubernetesInput{
		config: cfg,
		client: client.NewAPIClient(cfg),
	}
	count := 0
	i.Read(func(res *bindings.ReadResponse) error {
		count = count + 1

		result := client.Result{}
		json.Unmarshal(res.Data, &result)

		assert.Equal(t, "ADDED", result.Type, "Unexpected watch event type: %v", result.Type)
		return nil
	})

	assert.Equal(t, 1, count, "Expected 1 item, saw %v\n", count)
}
