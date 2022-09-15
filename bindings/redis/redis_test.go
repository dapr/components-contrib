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

	miniredis "github.com/alicebob/miniredis/v2"
	v8 "github.com/go-redis/redis/v8"
	v9 "github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	testData = `{"data":"data"}`
	testKey  = "test"
)

func TestInvokev8(t *testing.T) {
	s, c := setupMiniredisv8()
	defer s.Close()

	bind := &Redis{
		legacyRedis: true,
		clientv8:    c,
		logger:      logger.NewLogger("test"),
	}
	bind.ctx, bind.cancel = context.WithCancel(context.Background())

	_, err := c.Do(context.Background(), "GET", testKey).Result()
	assert.Equal(t, v8.Nil, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Data:     []byte(testData),
		Metadata: map[string]string{"key": testKey},
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, bindingRes == nil)

	getRes, err := c.Do(context.Background(), "GET", testKey).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, true, getRes == testData)
}

func TestInvokev9(t *testing.T) {
	s, c := setupMiniredisv9()
	defer s.Close()

	bind := &Redis{
		clientv9: c,
		logger:   logger.NewLogger("test"),
	}
	bind.ctx, bind.cancel = context.WithCancel(context.Background())

	_, err := c.Do(context.Background(), "GET", testKey).Result()
	assert.Equal(t, v9.Nil, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Data:     []byte(testData),
		Metadata: map[string]string{"key": testKey},
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, bindingRes == nil)

	getRes, err := c.Do(context.Background(), "GET", testKey).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, true, getRes == testData)
}

func setupMiniredisv8() (*miniredis.Miniredis, *v8.Client) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &v8.Options{
		Addr: s.Addr(),
		DB:   0,
	}

	return s, v8.NewClient(opts)
}

func setupMiniredisv9() (*miniredis.Miniredis, *v9.Client) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &v9.Options{
		Addr: s.Addr(),
		DB:   0,
	}

	return s, v9.NewClient(opts)
}
