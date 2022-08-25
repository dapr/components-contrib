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

package jetstream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func runServerWithOptions(opts server.Options) *server.Server {
	return natsserver.RunServer(&opts)
}

func runServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.Cluster.Name = "testing"
	return runServerWithOptions(opts)
}

func runDefaultServer() *server.Server {
	return runServerOnPort(nats.DefaultPort)
}

func newDefaultConnection(t tLogger) *nats.Conn {
	return newConnection(t, nats.DefaultPort)
}

func newConnection(t tLogger, port int) *nats.Conn {
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return nc
}

func connectAndCreateBucket(t *testing.T) (nats.KeyValue, *nats.Conn) {
	nc := newDefaultConnection(t)
	jsc, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Could not open jetstream: %v\n", err)
		return nil, nil
	}
	kv, err := jsc.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "test",
	})
	if err != nil {
		t.Fatalf("Could not open jetstream: %v\n", err)
		return nil, nil
	}
	return kv, nc
}

func TestDefaultConnection(t *testing.T) {
	s := runDefaultServer()
	defer s.Shutdown()

	_, nc := connectAndCreateBucket(t)
	defer nc.Close()
}

func TestSetGetAndDelete(t *testing.T) {
	s := runDefaultServer()
	defer s.Shutdown()

	_, nc := connectAndCreateBucket(t)
	nc.Close()

	store := NewJetstreamStateStore(nil)

	err := store.Init(state.Metadata{
		Base: metadata.Base{Properties: map[string]string{
			"natsURL": nats.DefaultURL,
			"bucket":  "test",
		}},
	})
	if err != nil {
		t.Fatalf("Could not init: %v\n", err)
		return
	}

	tkey := "key"
	tData := map[string]string{
		"dkey": "dvalue",
	}

	err = store.Set(&state.SetRequest{
		Key:   tkey,
		Value: tData,
	})
	if err != nil {
		t.Fatalf("Could not set: %v\n", err)
		return
	}

	resp, err := store.Get(&state.GetRequest{
		Key: tkey,
	})
	if err != nil {
		t.Fatalf("Could not get: %v\n", err)
		return
	}
	rData := make(map[string]string)
	json.Unmarshal(resp.Data, &rData)
	if !reflect.DeepEqual(rData, tData) {
		t.Fatal("Response data does not match written data\n")
	}

	err = store.Delete(&state.DeleteRequest{
		Key: tkey,
	})
	if err != nil {
		t.Fatalf("Could not delete: %v\n", err)
		return
	}

	_, err = store.Get(&state.GetRequest{
		Key: tkey,
	})
	if err == nil {
		t.Fatal("Could get after delete\n")
		return
	}
}
