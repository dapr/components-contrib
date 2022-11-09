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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStore_withDefaultBulkImpl(t *testing.T) {
	s := &Store1{}
	s.DefaultBulkStore = NewDefaultBulkStore(s)
	var store Store = s
	require.Equal(t, s.count, 0)
	require.Equal(t, s.bulkCount, 0)

	store.Get(&GetRequest{})
	store.Set(&SetRequest{})
	store.Delete(&DeleteRequest{})
	require.Equal(t, 3, s.count)
	require.Equal(t, 0, s.bulkCount)

	bulkGet, responses, err := store.BulkGet([]GetRequest{{}, {}, {}})
	require.Equal(t, false, bulkGet)
	require.Equal(t, 0, len(responses))
	require.NoError(t, err)
	require.Equal(t, 3, s.count)
	require.Equal(t, 0, s.bulkCount)
	store.BulkSet([]SetRequest{{}, {}, {}, {}})
	require.Equal(t, 3+4, s.count)
	require.Equal(t, 0, s.bulkCount)
	store.BulkDelete([]DeleteRequest{{}, {}, {}, {}, {}})
	require.Equal(t, 3+4+5, s.count)
	require.Equal(t, 0, s.bulkCount)
}

func TestStore_withCustomisedBulkImpl_notSupportBulkGet(t *testing.T) {
	s := &Store2{supportBulkGet: false}
	var store Store = s
	require.Equal(t, s.count, 0)
	require.Equal(t, s.bulkCount, 0)

	store.Get(&GetRequest{})
	store.Set(&SetRequest{})
	store.Delete(&DeleteRequest{})
	require.Equal(t, 3, s.count)
	require.Equal(t, 0, s.bulkCount)

	bulkGet, _, _ := store.BulkGet([]GetRequest{{}, {}, {}})
	require.Equal(t, false, bulkGet)
	require.Equal(t, 6, s.count)
	require.Equal(t, 0, s.bulkCount)
	store.BulkSet([]SetRequest{{}, {}, {}, {}})
	require.Equal(t, 6, s.count)
	require.Equal(t, 1, s.bulkCount)
	store.BulkDelete([]DeleteRequest{{}, {}, {}, {}, {}})
	require.Equal(t, 6, s.count)
	require.Equal(t, 2, s.bulkCount)
}

func TestStore_withCustomisedBulkImpl_supportBulkGet(t *testing.T) {
	s := &Store2{supportBulkGet: true}
	var store Store = s
	require.Equal(t, s.count, 0)
	require.Equal(t, s.bulkCount, 0)

	store.Get(&GetRequest{})
	store.Set(&SetRequest{})
	store.Delete(&DeleteRequest{})
	require.Equal(t, 3, s.count)
	require.Equal(t, 0, s.bulkCount)

	bulkGet, _, _ := store.BulkGet([]GetRequest{{}, {}, {}})
	require.Equal(t, true, bulkGet)
	require.Equal(t, 3, s.count)
	require.Equal(t, 1, s.bulkCount)
	store.BulkSet([]SetRequest{{}, {}, {}, {}})
	require.Equal(t, 3, s.count)
	require.Equal(t, 2, s.bulkCount)
	store.BulkDelete([]DeleteRequest{{}, {}, {}, {}, {}})
	require.Equal(t, 3, s.count)
	require.Equal(t, 3, s.bulkCount)
}

var (
	_ Store = &Store1{}
	_ Store = &Store2{}
)

// example of store which doesn't support bulk method.
type Store1 struct {
	DefaultBulkStore
	count     int
	bulkCount int
}

func (s *Store1) Init(metadata Metadata) error {
	return nil
}

func (s *Store1) Delete(req *DeleteRequest) error {
	s.count++

	return nil
}

func (s *Store1) Get(req *GetRequest) (*GetResponse, error) {
	s.count++

	return &GetResponse{}, nil
}

func (s *Store1) Set(req *SetRequest) error {
	s.count++

	return nil
}

func (s *Store1) GetComponentMetadata() map[string]string {
	return map[string]string{}
}

// example of store which supports bulk method.
type Store2 struct {
	// DefaultBulkStore
	count     int
	bulkCount int

	supportBulkGet bool
}

func (s *Store2) Init(metadata Metadata) error {
	return nil
}

func (s *Store2) Features() []Feature {
	return nil
}

func (s *Store2) Delete(req *DeleteRequest) error {
	s.count++

	return nil
}

func (s *Store2) Get(req *GetRequest) (*GetResponse, error) {
	s.count++

	return &GetResponse{}, nil
}

func (s *Store2) Set(req *SetRequest) error {
	s.count++

	return nil
}

func (s *Store2) BulkGet(req []GetRequest) (bool, []BulkGetResponse, error) {
	if s.supportBulkGet {
		s.bulkCount++

		return true, nil, nil
	}

	s.count += len(req)

	return false, nil, nil
}

func (s *Store2) BulkSet(req []SetRequest) error {
	s.bulkCount++

	return nil
}

func (s *Store2) BulkDelete(req []DeleteRequest) error {
	s.bulkCount++

	return nil
}

func (s *Store2) GetComponentMetadata() map[string]string {
	return map[string]string{}
}
