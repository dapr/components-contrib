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

package nacos

import (
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestInputBindingRead(t *testing.T) { //nolint:paralleltest
	m := bindings.Metadata{Name: "test", Properties: nil}
	var err error
	m.Properties, err = getNacosLocalCacheMetadata()
	require.NoError(t, err)
	n := NewNacos(logger.NewLogger("test"))
	err = n.Init(m)
	require.NoError(t, err)
	var count int32
	ch := make(chan bool, 1)

	handler := func(in *bindings.ReadResponse) ([]byte, error) {
		require.Equal(t, "hello", string(in.Data))
		atomic.AddInt32(&count, 1)
		ch <- true

		return nil, nil
	}

	go func() {
		err = n.Read(handler)
		require.NoError(t, err)
	}()

	select {
	case <-ch:
		require.Equal(t, int32(1), atomic.LoadInt32(&count))
	case <-time.After(time.Second):
		require.FailNow(t, "read timeout")
	}
}

func getNacosLocalCacheMetadata() (map[string]string, error) {
	tmpDir := "/tmp/config"
	dataID := "test"
	group := "DEFAULT_GROUP"

	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create dir failed. %w", err)
	}

	cfgFile := path.Join(tmpDir, fmt.Sprintf("%s@@%s@@", dataID, group))
	file, err := os.OpenFile(cfgFile, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil || file == nil {
		return nil, fmt.Errorf("open %s failed. %w", cfgFile, err)
	}

	defer func() {
		_ = file.Close()
	}()

	if _, err = file.WriteString("hello"); err != nil {
		return nil, fmt.Errorf("write file failed. %w", err)
	}

	return map[string]string{
		"cacheDir":   "/tmp", // default
		"nameServer": "localhost:8080/fake",
		"watches":    fmt.Sprintf("%s:%s", dataID, group),
	}, nil
}
