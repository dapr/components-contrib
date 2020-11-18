package state

import (
	"github.com/stretchr/testify/require"
	"testing"
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

	store.BulkGet([]GetRequest{GetRequest{}, GetRequest{}, GetRequest{}})
	require.Equal(t, 3 + 3, s.count)
	require.Equal(t, 0, s.bulkCount)
	store.BulkSet([]SetRequest{SetRequest{}, SetRequest{}, SetRequest{}, SetRequest{}})
	require.Equal(t, 3 + 3 +4, s.count)
	require.Equal(t, 0, s.bulkCount)
	store.BulkDelete([]DeleteRequest{DeleteRequest{}, DeleteRequest{}, DeleteRequest{}, DeleteRequest{}, DeleteRequest{}})
	require.Equal(t, 3 + 3 + 4 + 5, s.count)
	require.Equal(t, 0, s.bulkCount)
}

func TestStore_withCustomisedBulkImpl(t *testing.T) {
	s := &Store2{}
	var store Store = s
	require.Equal(t, s.count, 0)
	require.Equal(t, s.bulkCount, 0)

	store.Get(&GetRequest{})
	store.Set(&SetRequest{})
	store.Delete(&DeleteRequest{})
	require.Equal(t, 3, s.count)
	require.Equal(t, 0, s.bulkCount)

	store.BulkGet([]GetRequest{GetRequest{}, GetRequest{}, GetRequest{}})
	require.Equal(t, 3, s.count)
	require.Equal(t, 3, s.bulkCount)
	store.BulkSet([]SetRequest{SetRequest{}, SetRequest{}, SetRequest{}, SetRequest{}})
	require.Equal(t, 3, s.count)
	require.Equal(t, 3 +4, s.bulkCount)
	store.BulkDelete([]DeleteRequest{DeleteRequest{}, DeleteRequest{}, DeleteRequest{}, DeleteRequest{}, DeleteRequest{}})
	require.Equal(t, 3, s.count)
	require.Equal(t, 3 + 4 + 5, s.bulkCount)
}

var _ Store = &Store1{}
var _ Store = &Store2{}

// example of store which doesn't support bulk method
type Store1 struct {
	DefaultBulkStore
	count int
	bulkCount int
}

func (s *Store1) Init(metadata Metadata) error  {
	return nil
}

func (s *Store1) Delete(req *DeleteRequest) error  {
	s.count++
	return nil
}

func (s *Store1) Get(req *GetRequest) (*GetResponse, error)  {
	s.count++
	return &GetResponse{},nil
}

func (s *Store1) Set(req *SetRequest) error  {
	s.count++
	return nil
}

// example of store which supports bulk method
type Store2 struct {
	//DefaultBulkStore
	count int
	bulkCount int
}

func (s *Store2) Init(metadata Metadata) error  {
	return nil
}

func (s *Store2) Delete(req *DeleteRequest) error  {
	s.count++
	return nil
}

func (s *Store2) Get(req *GetRequest) (*GetResponse, error)  {
	s.count++
	return &GetResponse{},nil
}

func (s *Store2) Set(req *SetRequest) error  {
	s.count++
	return nil
}

func (s *Store2) BulkGet(req []GetRequest)  ([]GetResponse, error)  {
	s.bulkCount = s.bulkCount + len(req)
	return nil, nil
}

func (s *Store2) BulkSet(req []SetRequest) error {
	s.bulkCount = s.bulkCount + len(req)
	return nil
}

func (s *Store2) BulkDelete(req []DeleteRequest) error {
	s.bulkCount = s.bulkCount + len(req)
	return nil
}




