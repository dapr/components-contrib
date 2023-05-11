/*
Copyright 2023 The Dapr Authors
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

package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBulkStore(t *testing.T) {
	t.Run("default implementation", func(t *testing.T) {
		var (
			expectCount     int
			expectBulkCount int
		)

		s := &storeBulk{}
		s.BulkStore = NewDefaultBulkStore(s)
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.Get(context.Background(), &GetRequest{})
		s.Set(context.Background(), &SetRequest{})
		s.Delete(context.Background(), &DeleteRequest{})
		expectCount += 3
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		_, err := s.BulkGet(context.Background(), []GetRequest{{}, {}, {}}, BulkGetOpts{})
		require.NoError(t, err)
		expectCount += 3
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
		s.BulkSet(context.Background(), []SetRequest{{}, {}, {}, {}})
		expectCount += 4
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
		s.BulkDelete(context.Background(), []DeleteRequest{{}, {}, {}, {}, {}})
		expectCount += 5
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
	})

	t.Run("native bulk implementation", func(t *testing.T) {
		var (
			expectCount     int
			expectBulkCount int
		)

		s := &storeBulkNative{}
		s.BulkStore = NewDefaultBulkStore(s)

		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.Get(context.Background(), &GetRequest{})
		s.Set(context.Background(), &SetRequest{})
		s.Delete(context.Background(), &DeleteRequest{})
		expectCount += 3
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		_, _ = s.BulkGet(context.Background(), []GetRequest{{}, {}, {}}, BulkGetOpts{})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
		s.BulkSet(context.Background(), []SetRequest{{}, {}, {}, {}})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
		s.BulkDelete(context.Background(), []DeleteRequest{{}, {}, {}, {}, {}})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
	})
}

var (
	_ Store = &storeBulk{}
	_ Store = &storeBulkNative{}
)

// example of a store which doesn't support native bulk methods
type storeBulk struct {
	BulkStore

	count     int
	bulkCount int
}

func (s *storeBulk) Init(ctx context.Context, metadata Metadata) error {
	return nil
}

func (s *storeBulk) Delete(ctx context.Context, req *DeleteRequest) error {
	s.count++
	return nil
}

func (s *storeBulk) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	s.count++
	return &GetResponse{}, nil
}

func (s *storeBulk) Set(ctx context.Context, req *SetRequest) error {
	s.count++
	return nil
}

func (s *storeBulk) GetComponentMetadata() map[string]string {
	return map[string]string{}
}

func (s *storeBulk) Features() []Feature {
	return nil
}

// example of a store which supports native bulk methods
type storeBulkNative struct {
	storeBulk
}

func (s *storeBulkNative) BulkGet(ctx context.Context, req []GetRequest, opts BulkGetOpts) ([]BulkGetResponse, error) {
	s.bulkCount++
	return nil, nil
}

func (s *storeBulkNative) BulkSet(ctx context.Context, req []SetRequest) error {
	s.bulkCount++
	return nil
}

func (s *storeBulkNative) BulkDelete(ctx context.Context, req []DeleteRequest) error {
	s.bulkCount++
	return nil
}
