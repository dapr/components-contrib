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
	"testing"

	"github.com/stretchr/testify/require"
)

var simulatedError = errors.New("simulated")

func TestBulkStore(t *testing.T) {
	t.Run("default implementation", func(t *testing.T) {
		var (
			expectCount     int
			expectBulkCount int
		)

		ctx := context.Background()

		s := &storeBulk{}
		s.BulkStore = NewDefaultBulkStore(s)
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.Get(ctx, &GetRequest{})
		s.Set(ctx, &SetRequest{})
		s.Delete(ctx, &DeleteRequest{})
		expectCount += 3
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		_, err := s.BulkGet(ctx, []GetRequest{{}, {}, {}}, BulkGetOpts{})
		require.NoError(t, err)
		expectCount += 3
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.BulkSet(ctx, []SetRequest{{}, {}, {}, {}})
		expectCount += 4
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.BulkDelete(ctx, []DeleteRequest{{}, {}, {}, {}, {}})
		expectCount += 5
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		// Test errors
		err = s.Set(ctx, &SetRequest{Key: "error-key"})
		require.Error(t, err)
		expectCount++
		require.Equal(t, simulatedError, err)
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		err = s.BulkSet(ctx, []SetRequest{{Key: "error-key"}, {}, {Key: "error-key"}, {}})
		expectCount += 4
		require.Error(t, err)
		merr, ok := err.(interface{ Unwrap() []error })
		require.True(t, ok)
		errs := merr.Unwrap()
		require.Len(t, errs, 2)
		require.ErrorIs(t, errs[0], simulatedError)
		require.ErrorIs(t, errs[1], simulatedError)
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)
	})

	t.Run("native bulk implementation", func(t *testing.T) {
		var (
			expectCount     int
			expectBulkCount int
		)

		ctx := context.Background()

		s := &storeBulkNative{}
		s.BulkStore = NewDefaultBulkStore(s)

		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.Get(ctx, &GetRequest{})
		s.Set(ctx, &SetRequest{})
		s.Delete(ctx, &DeleteRequest{})
		expectCount += 3
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		_, _ = s.BulkGet(ctx, []GetRequest{{}, {}, {}}, BulkGetOpts{})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.BulkSet(ctx, []SetRequest{{}, {}, {}, {}})
		expectBulkCount += 1
		require.Equal(t, expectCount, s.count)
		require.Equal(t, expectBulkCount, s.bulkCount)

		s.BulkDelete(ctx, []DeleteRequest{{}, {}, {}, {}, {}})
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
	if req.Key == "error-key" {
		return simulatedError
	}
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
