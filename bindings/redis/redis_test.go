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

package redis

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	internalredis "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	testData = `{"data":"data"}`
	testKey  = "test"
)

func TestInvokeCreate(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	// miniRedis is compatible with the existing v8 client
	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	_, err := c.DoRead(context.Background(), "GET", testKey)
	assert.Equal(t, redis.Nil, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Data:      []byte(testData),
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.CreateOperation,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, bindingRes == nil)

	getRes, err := c.DoRead(context.Background(), "GET", testKey)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, getRes == testData)
}

func TestInvokeGetWithoutDeleteFlag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	err := c.DoWrite(context.Background(), "SET", testKey, testData)
	assert.Equal(t, nil, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, string(bindingRes.Data) == testData)

	bindingResGet, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})

	assert.Equal(t, nil, err)

	assert.Equal(t, true, string(bindingResGet.Data) == testData)
}

func TestInvokeGetWithDeleteFlag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	err := c.DoWrite(context.Background(), "SET", testKey, testData)
	assert.Equal(t, nil, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey, "delete": "true"},
		Operation: bindings.GetOperation,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, string(bindingRes.Data) == testData)

	bindingResGet, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})

	assert.Equal(t, nil, err)

	assert.Equal(t, []byte(nil), bindingResGet.Data)
}

func TestInvokeDelete(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	err := c.DoWrite(context.Background(), "SET", testKey, testData)
	assert.Equal(t, nil, err)

	getRes, err := c.DoRead(context.Background(), "GET", testKey)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, getRes == testData)

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.DeleteOperation,
	})

	assert.Equal(t, nil, err)

	rgetRep, err := c.DoRead(context.Background(), "GET", testKey)
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, nil, rgetRep)
}

func TestCreateExpire(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}
	_, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey, metadata.TTLMetadataKey: "1"},
		Operation: bindings.CreateOperation,
		Data:      []byte(testData),
	})
	assert.Equal(t, nil, err)

	rgetRep, err := c.DoRead(context.Background(), "TTL", testKey)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), rgetRep)

	res, err2 := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})
	assert.Equal(t, nil, err2)
	assert.Equal(t, res.Data, []byte(testData))

	// wait for ttl to expire
	s.FastForward(2 * time.Second)

	res, err2 = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})
	assert.Nil(t, err2)
	assert.Equal(t, []byte(nil), res.Data)

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.DeleteOperation,
	})
	assert.Equal(t, nil, err)
}

func TestIncrement(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}
	_, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: IncrementOperation,
	})
	assert.Equal(t, nil, err)

	res, err2 := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.GetOperation,
	})
	assert.Nil(t, nil, err2)
	assert.Equal(t, res.Data, []byte("1"))

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey", metadata.TTLMetadataKey: "5"},
		Operation: IncrementOperation,
	})
	assert.Nil(t, err)

	rgetRep, err := c.DoRead(context.Background(), "TTL", "incKey")
	assert.Nil(t, err)
	assert.Equal(t, int64(5), rgetRep)

	res, err2 = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.GetOperation,
	})
	assert.Equal(t, nil, err2)
	assert.Equal(t, []byte("2"), res.Data)

	// wait for ttl to expire
	s.FastForward(10 * time.Second)

	res, err2 = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.GetOperation,
	})
	assert.Nil(t, err2)
	assert.Equal(t, []byte(nil), res.Data)

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.DeleteOperation,
	})
	assert.Equal(t, nil, err)
}

func setupMiniredis() (*miniredis.Miniredis, internalredis.RedisClient) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &redis.Options{
		Addr: s.Addr(),
		DB:   0,
	}

	return s, internalredis.ClientFromV8Client(redis.NewClient(opts))
}
