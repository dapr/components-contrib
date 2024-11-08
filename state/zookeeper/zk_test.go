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

package zookeeper

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/ptr"
)

//go:generate mockgen -package zookeeper -source zk.go -destination zk_mock.go

// newConfig.
func TestNewConfig(t *testing.T) {
	t.Run("With all required fields", func(t *testing.T) {
		properties := map[string]string{
			"servers":        "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002",
			"sessionTimeout": "5s",
		}
		cp, err := newConfig(properties)
		require.NoError(t, err, "Unexpected error: %v", err)
		assert.NotNil(t, cp, "failed to respond to missing data field")
		assert.Equal(t, []string{
			"127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3002",
		}, cp.servers, "failed to get servers")
		assert.Equal(t, 5*time.Second, cp.sessionTimeout, "failed to get DialTimeout")
	})

	t.Run("With all required fields", func(t *testing.T) {
		props := &properties{
			Servers:        "localhost:3000",
			SessionTimeout: "5s",
		}
		_, err := props.parse()
		require.NoError(t, err, "failed to read all fields")
	})
	t.Run("With missing servers", func(t *testing.T) {
		props := &properties{
			SessionTimeout: "5s",
		}
		_, err := props.parse()
		require.Error(t, err, "failed to get missing endpoints error")
	})
	t.Run("With missing sessionTimeout", func(t *testing.T) {
		props := &properties{
			Servers: "localhost:3000",
		}
		_, err := props.parse()
		require.Error(t, err, "failed to get invalid sessionTimeout error")
	})
}

// Get.
func TestGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	s := StateStore{conn: conn}

	t.Run("With key exists", func(t *testing.T) {
		conn.EXPECT().Get("foo").Return([]byte("bar"), &zk.Stat{Version: 123}, nil).Times(1)

		res, err := s.Get(context.Background(), &state.GetRequest{Key: "foo"})
		assert.NotNil(t, res, "Key must be exists")
		assert.Equal(t, "bar", string(res.Data), "Value must be equals")
		assert.Equal(t, ptr.Of("123"), res.ETag, "ETag must be equals")
		require.NoError(t, err, "Key must be exists")
	})

	t.Run("With key non-exists", func(t *testing.T) {
		conn.EXPECT().Get("foo").Return(nil, nil, zk.ErrNoNode).Times(1)

		res, err := s.Get(context.Background(), &state.GetRequest{Key: "foo"})
		assert.Equal(t, &state.GetResponse{}, res, "Response must be empty")
		require.NoError(t, err, "Non-existent key must not be treated as error")
	})
}

// Delete.
func TestDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	s := StateStore{conn: conn}

	etag := "123"
	t.Run("With key", func(t *testing.T) {
		conn.EXPECT().Delete("foo", int32(anyVersion)).Return(nil).Times(1)

		err := s.Delete(context.Background(), &state.DeleteRequest{Key: "foo"})
		require.NoError(t, err, "Key must be exists")
	})

	t.Run("With key and version", func(t *testing.T) {
		conn.EXPECT().Delete("foo", int32(123)).Return(nil).Times(1)

		err := s.Delete(context.Background(), &state.DeleteRequest{Key: "foo", ETag: &etag})
		require.NoError(t, err, "Key must be exists")
	})

	t.Run("With key and concurrency", func(t *testing.T) {
		conn.EXPECT().Delete("foo", int32(anyVersion)).Return(nil).Times(1)

		err := s.Delete(context.Background(), &state.DeleteRequest{
			Key:     "foo",
			ETag:    &etag,
			Options: state.DeleteStateOption{Concurrency: state.LastWrite},
		})
		require.NoError(t, err, "Key must be exists")
	})

	t.Run("With delete error", func(t *testing.T) {
		conn.EXPECT().Delete("foo", int32(anyVersion)).Return(zk.ErrUnknown).Times(1)

		err := s.Delete(context.Background(), &state.DeleteRequest{Key: "foo"})
		require.EqualError(t, err, "zk: unknown error")
	})

	t.Run("With delete and ignore NoNode error", func(t *testing.T) {
		conn.EXPECT().Delete("foo", int32(anyVersion)).Return(zk.ErrNoNode).Times(1)

		err := s.Delete(context.Background(), &state.DeleteRequest{Key: "foo"})
		require.NoError(t, err, "Delete must be successful")
	})
}

// Set.
func TestSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := NewMockConn(ctrl)
	s := StateStore{conn: conn}

	stat := &zk.Stat{}

	etag := "123"
	t.Run("With key", func(t *testing.T) {
		conn.EXPECT().Set("foo", []byte("\"bar\""), int32(anyVersion)).Return(stat, nil).Times(1)

		err := s.Set(context.Background(), &state.SetRequest{Key: "foo", Value: "bar"})
		require.NoError(t, err, "Key must be set")
	})
	t.Run("With key and version", func(t *testing.T) {
		conn.EXPECT().Set("foo", []byte("\"bar\""), int32(123)).Return(stat, nil).Times(1)

		err := s.Set(context.Background(), &state.SetRequest{Key: "foo", Value: "bar", ETag: &etag})
		require.NoError(t, err, "Key must be set")
	})
	t.Run("With key and concurrency", func(t *testing.T) {
		conn.EXPECT().Set("foo", []byte("\"bar\""), int32(anyVersion)).Return(stat, nil).Times(1)

		err := s.Set(context.Background(), &state.SetRequest{
			Key:     "foo",
			Value:   "bar",
			ETag:    &etag,
			Options: state.SetStateOption{Concurrency: state.LastWrite},
		})
		require.NoError(t, err, "Key must be set")
	})

	t.Run("With error", func(t *testing.T) {
		conn.EXPECT().Set("foo", []byte("\"bar\""), int32(anyVersion)).Return(nil, zk.ErrUnknown).Times(1)

		err := s.Set(context.Background(), &state.SetRequest{Key: "foo", Value: "bar"})
		require.EqualError(t, err, "zk: unknown error")
	})
	t.Run("With NoNode error and retry", func(t *testing.T) {
		conn.EXPECT().Set("foo", []byte("\"bar\""), int32(anyVersion)).Return(nil, zk.ErrNoNode).Times(1)
		conn.EXPECT().Create("foo", []byte("\"bar\""), int32(0), nil).Return("/foo", nil).Times(1)

		err := s.Set(context.Background(), &state.SetRequest{Key: "foo", Value: "bar"})
		require.NoError(t, err, "Key must be create")
	})
}
