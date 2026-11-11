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
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
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

	store := NewJetstreamStateStore(logger.NewLogger("test"))
	defer store.Close()

	err := store.Init(t.Context(), state.Metadata{
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

	err = store.Set(t.Context(), &state.SetRequest{
		Key:   tkey,
		Value: tData,
	})
	if err != nil {
		t.Fatalf("Could not set: %v\n", err)
		return
	}

	resp, err := store.Get(t.Context(), &state.GetRequest{
		Key: tkey,
	})
	if err != nil {
		t.Fatalf("Could not get: %v\n", err)
		return
	}
	rData := make(map[string]string)
	_ = json.Unmarshal(resp.Data, &rData)
	if !reflect.DeepEqual(rData, tData) {
		t.Fatal("Response data does not match written data\n")
	}

	err = store.Delete(t.Context(), &state.DeleteRequest{
		Key: tkey,
	})
	if err != nil {
		t.Fatalf("Could not delete: %v\n", err)
		return
	}

	_, err = store.Get(t.Context(), &state.GetRequest{
		Key: tkey,
	})
	if err == nil {
		t.Fatal("Could get after delete\n")
		return
	}
}

func initStore(t *testing.T, url string, natsOpts ...nats.Option) *StateStore {
	t.Helper()

	store := NewJetstreamStateStore(logger.NewLogger("test")).(*StateStore)
	store.natsOpts = natsOpts
	t.Cleanup(func() { store.Close() })

	err := store.Init(t.Context(), state.Metadata{
		Base: metadata.Base{Properties: map[string]string{
			"natsURL": url,
			"bucket":  "test",
		}},
	})
	if err != nil {
		t.Fatalf("Could not init: %v\n", err)
	}
	return store
}

func runFileStoreServer(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = storeDir
	s := runServerWithOptions(opts)
	t.Cleanup(s.Shutdown)
	return s
}

func TestInitReconnectsIndefinitely(t *testing.T) {
	s := runFileStoreServer(t, -1, t.TempDir())

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Could not connect: %v\n", err)
	}
	jsc, _ := nc.JetStream()
	if _, err = jsc.CreateKeyValue(&nats.KeyValueConfig{Bucket: "test"}); err != nil {
		t.Fatalf("Could not create bucket: %v\n", err)
	}
	nc.Close()

	store := initStore(t, s.ClientURL())

	// nats.go closes the connection for good once MaxReconnect attempts are
	// used up (60 by default), and nothing re-creates the store after that.
	if got := store.nc.Opts.MaxReconnect; got != -1 {
		t.Fatalf("MaxReconnect = %d, want -1", got)
	}
}

// TestRecoversAfterLongOutage stops the server for longer than nats.go's
// default reconnect budget would allow, then starts it again on the same port
// with the same JetStream state. The store must serve Get and Set again
// without being re-initialized.
func TestRecoversAfterLongOutage(t *testing.T) {
	const (
		reconnectWait = 10 * time.Millisecond
		// With nats.go's default MaxReconnect of 60, the connection is
		// closed after roughly 60*reconnectWait. Stay down well past that.
		outage = 60 * reconnectWait * 3
	)

	storeDir := t.TempDir()
	s := runFileStoreServer(t, -1, storeDir)
	port := s.Addr().(*net.TCPAddr).Port

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Could not connect: %v\n", err)
	}
	jsc, _ := nc.JetStream()
	_, err = jsc.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "test",
		Storage: nats.FileStorage,
	})
	if err != nil {
		t.Fatalf("Could not create bucket: %v\n", err)
	}
	nc.Close()

	store := initStore(t, s.ClientURL(),
		nats.ReconnectWait(reconnectWait),
		nats.ReconnectJitter(0, 0),
	)

	if err = store.Set(t.Context(), &state.SetRequest{Key: "before", Value: "v1"}); err != nil {
		t.Fatalf("Could not set before outage: %v\n", err)
	}

	s.Shutdown()
	s.WaitForShutdown()
	time.Sleep(outage)

	if store.nc.IsClosed() {
		t.Error("connection was closed during the outage")
	}

	runFileStoreServer(t, port, storeDir)

	// The first request after the restart may race the reconnect.
	deadline := time.Now().Add(10 * time.Second)
	for {
		err = store.Set(t.Context(), &state.SetRequest{Key: "after", Value: "v2"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Could not set after outage: %v\n", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := store.Get(t.Context(), &state.GetRequest{Key: "before"})
	if err != nil {
		t.Fatalf("Could not get after outage: %v\n", err)
	}
	if string(resp.Data) != `"v1"` {
		t.Fatalf("Get(before) = %s, want \"v1\"", resp.Data)
	}
}
