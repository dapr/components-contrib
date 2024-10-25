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
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
)

var errSimulated = errors.New("simulated")

func TestBulkStore(t *testing.T) {
	t.Run("default implementation", func(t *testing.T) {
		var (
			expectCount     int32
			expectBulkCount int32
		)

		ctx := context.Background()

		s := &storeBulk{}
		s.BulkStore = NewDefaultBulkStore(s)
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		s.Get(ctx, &GetRequest{})
		s.Set(ctx, &SetRequest{})
		s.Delete(ctx, &DeleteRequest{})
		expectCount += 3
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		_, err := s.BulkGet(ctx, []GetRequest{{}, {}, {}}, BulkGetOpts{})
		require.NoError(t, err)
		expectCount += 3
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		s.BulkSet(ctx, []SetRequest{{}, {}, {}, {}}, BulkStoreOpts{})
		expectCount += 4
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		s.BulkDelete(ctx, []DeleteRequest{{}, {}, {}, {}, {}}, BulkStoreOpts{})
		expectCount += 5
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		// Test errors
		err = s.Set(ctx, &SetRequest{Key: "error-key"})
		require.Error(t, err)
		expectCount++
		require.Equal(t, errSimulated, err)
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		err = s.BulkSet(ctx, []SetRequest{{Key: "error-key1"}, {}, {Key: "error-key2"}, {}}, BulkStoreOpts{})
		expectCount += 4
		require.Error(t, err)
		merr, ok := err.(interface{ Unwrap() []error })
		require.True(t, ok)
		errs := merr.Unwrap()
		require.Len(t, errs, 2)
		for i := range 2 {
			var bse BulkStoreError
			require.ErrorAs(t, errs[i], &bse)
			assert.True(t, bse.key == "error-key1" || bse.key == "error-key2")
			require.ErrorIs(t, bse, errSimulated)
			require.ErrorIs(t, errs[i], errSimulated)
		}
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())
	})

	t.Run("native bulk implementation", func(t *testing.T) {
		var (
			expectCount     int32
			expectBulkCount int32
		)

		ctx := context.Background()

		s := &storeBulkNative{}
		s.BulkStore = NewDefaultBulkStore(s)

		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		s.Get(ctx, &GetRequest{})
		s.Set(ctx, &SetRequest{})
		s.Delete(ctx, &DeleteRequest{})
		expectCount += 3
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		_, _ = s.BulkGet(ctx, []GetRequest{{}, {}, {}}, BulkGetOpts{})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		s.BulkSet(ctx, []SetRequest{{}, {}, {}, {}}, BulkStoreOpts{})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())

		s.BulkDelete(ctx, []DeleteRequest{{}, {}, {}, {}, {}}, BulkStoreOpts{})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count.Load())
		require.Equal(t, expectBulkCount, s.bulkCount.Load())
	})
}

var (
	_ Store = &storeBulk{}
	_ Store = &storeBulkNative{}
)

// example of a store which doesn't support native bulk methods
type storeBulk struct {
	BulkStore

	count     atomic.Int32
	bulkCount atomic.Int32
}

func (s *storeBulk) Init(ctx context.Context, metadata Metadata) error {
	return nil
}

func (s *storeBulk) Delete(ctx context.Context, req *DeleteRequest) error {
	s.count.Add(1)
	return nil
}

func (s *storeBulk) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	s.count.Add(1)
	return &GetResponse{}, nil
}

func (s *storeBulk) Set(ctx context.Context, req *SetRequest) error {
	s.count.Add(1)
	if strings.Contains(req.Key, "error-key") {
		return errSimulated
	}
	return nil
}

func (s *storeBulk) Close() error {
	return nil
}

func (s *storeBulk) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	return
}

func (s *storeBulk) Features() []Feature {
	return nil
}

// example of a store which supports native bulk methods
type storeBulkNative struct {
	storeBulk
}

func (s *storeBulkNative) BulkGet(ctx context.Context, req []GetRequest, opts BulkGetOpts) ([]BulkGetResponse, error) {
	s.bulkCount.Add(1)
	return nil, nil
}

func (s *storeBulkNative) BulkSet(ctx context.Context, req []SetRequest, _ BulkStoreOpts) error {
	s.bulkCount.Add(1)
	return nil
}

func (s *storeBulkNative) BulkDelete(ctx context.Context, req []DeleteRequest, _ BulkStoreOpts) error {
	s.bulkCount.Add(1)
	return nil
}
