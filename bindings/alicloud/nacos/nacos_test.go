// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nacos

import (
	"fmt"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"Endpoint": "a", "Region": "b", "Namespace": "c", "AccessKey": "d", "SecretKey": "e",
	}
	meta, err := parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", meta.Endpoint)
	assert.Equal(t, "b", meta.RegionID)
	assert.Equal(t, "c", meta.NamespaceID)
	assert.Equal(t, "d", meta.AccessKey)
	assert.Equal(t, "e", meta.SecretKey)
}

func TestInputBindingRead(t *testing.T) {
	m := bindings.Metadata{}
	var err error
	m.Properties, err = getNacosLocalCacheMetadata()
	require.NoError(t, err)

	n := NewNacos(logger.NewLogger("test"))
	err = n.Init(m)
	require.NoError(t, err)

	var count int32
	handler := func(in *bindings.ReadResponse) ([]byte, error) {
		require.Equal(t, "hello", string(in.Data))
		atomic.AddInt32(&count, 1)
		return nil, nil
	}
	go func() {
		err = n.Read(handler)
		require.NoError(t, err)
	}()

	time.Sleep(time.Second)
	require.Equal(t, int32(1), atomic.LoadInt32(&count))
}

func getNacosLocalCacheMetadata() (map[string]string, error) {
	tmpDir := "/tmp/config"
	dataId := "test"
	group := "DEFAULT_GROUP"

	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return nil, err
	}

	cfgFile := path.Join(tmpDir, fmt.Sprintf("%s@@%s@@", dataId, group))
	file, err := os.OpenFile(cfgFile, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil || file == nil {
		return nil, fmt.Errorf("open %s failed. %v", cfgFile, err)
	}

	defer func() {
		_ = file.Close()
	}()

	if _, err = file.WriteString("hello"); err != nil {
		return nil, err
	}

	return map[string]string{
		"cacheDir":   "/tmp", // default
		"nameServer": "localhost:8080/fake",
		"watches":    fmt.Sprintf("%s:%s", dataId, group),
	}, nil
}
