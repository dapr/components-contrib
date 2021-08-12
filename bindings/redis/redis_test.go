// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	testData = `{"data":"data"}`
	testKey  = "test"
)

func TestInvoke(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}
	bind.ctx, bind.cancel = context.WithCancel(context.Background())

	_, err := c.Do(context.Background(), "GET", testKey).Result()
	assert.Equal(t, redis.Nil, err)

	bindingRes, err := bind.Invoke(&bindings.InvokeRequest{
		Data:     []byte(testData),
		Metadata: map[string]string{"key": testKey},
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, bindingRes == nil)

	getRes, err := c.Do(context.Background(), "GET", testKey).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, true, getRes == testData)
}

func setupMiniredis() (*miniredis.Miniredis, *redis.Client) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &redis.Options{
		Addr: s.Addr(),
		DB:   0,
	}

	return s, redis.NewClient(opts)
}
