/*
Copyright 2026 The Dapr Authors
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

package etcd

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type fakeKV struct {
	clientv3.KV

	succeeded bool
	commitErr error
	onCommit  func()
	txns      []*fakeTxn
}

type fakeLease struct {
	clientv3.Lease

	grantErrAfter int
	granted       []int64
	revoked       []clientv3.LeaseID
}

func (f *fakeLease) Grant(_ context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	if f.grantErrAfter > 0 && len(f.granted) == f.grantErrAfter {
		return nil, errors.New("grant failed")
	}
	f.granted = append(f.granted, ttl)
	return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(len(f.granted)), TTL: ttl}, nil
}

func (f *fakeLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.revoked = append(f.revoked, id)
	return &clientv3.LeaseRevokeResponse{}, nil
}

type fakeTxn struct {
	kv   *fakeKV
	cmps []clientv3.Cmp
	ops  []clientv3.Op
}

func (f *fakeKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return nil, errors.New("unexpected pre-read")
}

func (f *fakeKV) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

func (f *fakeKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (f *fakeKV) Txn(context.Context) clientv3.Txn {
	txn := &fakeTxn{kv: f}
	f.txns = append(f.txns, txn)
	return txn
}

func (t *fakeTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	t.cmps = append(t.cmps, cs...)
	return t
}

func (t *fakeTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *fakeTxn) Else(...clientv3.Op) clientv3.Txn {
	return t
}

func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.kv.onCommit != nil {
		t.kv.onCommit()
	}
	if t.kv.commitErr != nil {
		return nil, t.kv.commitErr
	}
	return &clientv3.TxnResponse{Succeeded: t.kv.succeeded}, nil
}

func newFakeStore(succeeded bool) (*Etcd, *fakeKV) {
	s, kv, _ := newFakeLeaseStore(succeeded)
	return s, kv
}

func newFakeLeaseStore(succeeded bool) (*Etcd, *fakeKV, *fakeLease) {
	kv := &fakeKV{succeeded: succeeded}
	lease := &fakeLease{}
	return &Etcd{
		client:        &clientv3.Client{KV: kv, Lease: lease},
		keyPrefixPath: "p",
		schema:        schemaV1{},
		maxTxnOps:     128,
		logger:        logger.NewLogger("test"),
	}, kv, lease
}

func requireETagKind(t *testing.T, err error, kind state.ETagErrorKind) {
	t.Helper()
	var etagErr *state.ETagError
	require.ErrorAs(t, err, &etagErr)
	assert.Equal(t, kind, etagErr.Kind())
}

func TestMultiSingleTxn(t *testing.T) {
	s, kv := newFakeStore(false)

	err := s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
		state.SetRequest{Key: "a", Value: "a"},
		state.SetRequest{Key: "b", Value: "b", ETag: ptr.Of("5")},
		state.DeleteRequest{Key: "c", ETag: ptr.Of("7")},
	}})
	requireETagKind(t, err, state.ETagMismatch)

	require.Len(t, kv.txns, 1)
	txn := kv.txns[0]
	require.Len(t, txn.cmps, 2)
	assert.Equal(t, "p/b", string(txn.cmps[0].KeyBytes()))
	assert.Equal(t, "MOD", txn.cmps[0].Target.String())
	assert.Equal(t, "p/c", string(txn.cmps[1].KeyBytes()))
	assert.Equal(t, "MOD", txn.cmps[1].Target.String())
	require.Len(t, txn.ops, 3)
	for _, op := range txn.ops {
		assert.False(t, op.IsTxn())
	}
}

func TestNoPreRead(t *testing.T) {
	s, kv := newFakeStore(true)
	firstWrite := state.SetStateOption{Concurrency: state.FirstWrite}

	require.NoError(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("5")}))
	require.NoError(t, s.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("5")}))
	require.NoError(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
		state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("5")},
		state.SetRequest{Key: "b", Value: "b", Options: firstWrite},
	}}))

	require.Len(t, kv.txns, 3)
	cmps := kv.txns[2].cmps
	require.Len(t, cmps, 2)
	assert.Equal(t, "p/b", string(cmps[1].KeyBytes()))
	assert.Equal(t, "CREATE", cmps[1].Target.String())
}

func TestETagParsing(t *testing.T) {
	s, kv := newFakeStore(true)

	requireETagKind(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("nope")}), state.ETagInvalid)
	requireETagKind(t, s.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("nope")}), state.ETagInvalid)

	for _, etag := range []string{"0", "-3"} {
		requireETagKind(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of(etag)}), state.ETagMismatch)
		requireETagKind(t, s.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of(etag)}), state.ETagMismatch)
	}

	assert.Empty(t, kv.txns)
}

func TestSetDeleteCheckSucceeded(t *testing.T) {
	s, _ := newFakeStore(false)

	requireETagKind(t, s.Set(t.Context(), &state.SetRequest{Key: "a", Value: "a", ETag: ptr.Of("5")}), state.ETagMismatch)
	requireETagKind(t, s.Delete(t.Context(), &state.DeleteRequest{Key: "a", ETag: ptr.Of("5")}), state.ETagMismatch)
}

func TestMultiSameKey(t *testing.T) {
	t.Run("only the last op is sent", func(t *testing.T) {
		s, kv := newFakeStore(true)
		require.NoError(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "k", Value: "1", ETag: ptr.Of("5")},
			state.SetRequest{Key: "k", Value: "2"},
			state.DeleteRequest{Key: "x"},
			state.SetRequest{Key: "x", Value: "3"},
		}}))

		require.Len(t, kv.txns, 1)
		txn := kv.txns[0]
		require.Len(t, txn.cmps, 1)
		assert.Equal(t, "p/k", string(txn.cmps[0].KeyBytes()))
		require.Len(t, txn.ops, 2)
		assert.True(t, txn.ops[0].IsPut())
		assert.Equal(t, "p/k", string(txn.ops[0].KeyBytes()))
		assert.JSONEq(t, `"2"`, string(txn.ops[0].ValueBytes()))
		assert.True(t, txn.ops[1].IsPut())
		assert.Equal(t, "p/x", string(txn.ops[1].KeyBytes()))
	})

	t.Run("etag after an earlier write of the key mismatches", func(t *testing.T) {
		s, kv := newFakeStore(true)
		requireETagKind(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "k", Value: "1", ETag: ptr.Of("5")},
			state.DeleteRequest{Key: "k", ETag: ptr.Of("5")},
		}}), state.ETagMismatch)
		assert.Empty(t, kv.txns)
	})

	t.Run("first-write after an earlier set mismatches", func(t *testing.T) {
		s, kv := newFakeStore(true)
		requireETagKind(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "k", Value: "1"},
			state.SetRequest{Key: "k", Value: "2", Options: state.SetStateOption{Concurrency: state.FirstWrite}},
		}}), state.ETagMismatch)
		assert.Empty(t, kv.txns)
	})

	t.Run("first-write after an earlier delete needs no compare", func(t *testing.T) {
		s, kv := newFakeStore(true)
		require.NoError(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{Key: "k", ETag: ptr.Of("5")},
			state.SetRequest{Key: "k", Value: "2", Options: state.SetStateOption{Concurrency: state.FirstWrite}},
		}}))
		require.Len(t, kv.txns, 1)
		require.Len(t, kv.txns[0].cmps, 1)
		assert.Equal(t, "MOD", kv.txns[0].cmps[0].Target.String())
		require.Len(t, kv.txns[0].ops, 1)
		assert.True(t, kv.txns[0].ops[0].IsPut())
	})
}

func TestMultiLeases(t *testing.T) {
	ttl := func(s string) map[string]string { return map[string]string{"ttlInSeconds": s} }
	ops := []state.TransactionalStateOperation{
		state.SetRequest{Key: "a", Value: "a", Metadata: ttl("60")},
		state.SetRequest{Key: "b", Value: "b", Metadata: ttl("60")},
		state.SetRequest{Key: "c", Value: "c", Metadata: ttl("30")},
		state.SetRequest{Key: "d", Value: "d"},
		state.SetRequest{Key: "e", Value: "e", ETag: ptr.Of("5"), Metadata: ttl("60")},
	}

	t.Run("one lease per ttl, kept on success", func(t *testing.T) {
		s, kv, lease := newFakeLeaseStore(true)
		require.NoError(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops}))
		assert.Equal(t, []int64{60, 30}, lease.granted)
		assert.Empty(t, lease.revoked)

		require.Len(t, kv.txns, 1)
		leaseOf := func(i int) clientv3.LeaseID {
			return clientv3.LeaseID(reflect.ValueOf(kv.txns[0].ops[i]).FieldByName("leaseID").Int())
		}
		assert.Equal(t, clientv3.LeaseID(1), leaseOf(0))
		assert.Equal(t, clientv3.LeaseID(1), leaseOf(1))
		assert.Equal(t, clientv3.LeaseID(2), leaseOf(2))
		assert.Equal(t, clientv3.NoLease, leaseOf(3))
		assert.Equal(t, clientv3.LeaseID(1), leaseOf(4))
	})

	t.Run("revoked on etag mismatch", func(t *testing.T) {
		s, _, lease := newFakeLeaseStore(false)
		requireETagKind(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops}), state.ETagMismatch)
		assert.ElementsMatch(t, []clientv3.LeaseID{1, 2}, lease.revoked)
	})

	t.Run("revoked on etag mismatch after the request context is done", func(t *testing.T) {
		s, kv, lease := newFakeLeaseStore(false)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		kv.onCommit = cancel
		requireETagKind(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: ops}), state.ETagMismatch)
		assert.ElementsMatch(t, []clientv3.LeaseID{1, 2}, lease.revoked)
	})

	t.Run("revoked when a later grant fails", func(t *testing.T) {
		s, kv, lease := newFakeLeaseStore(true)
		lease.grantErrAfter = 1
		require.ErrorContains(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops}), "grant failed")
		assert.Equal(t, []clientv3.LeaseID{1}, lease.revoked)
		assert.Empty(t, kv.txns)
	})

	t.Run("kept on commit error", func(t *testing.T) {
		s, kv, lease := newFakeLeaseStore(true)
		kv.commitErr = errors.New("commit failed")
		require.ErrorContains(t, s.Multi(t.Context(), &state.TransactionalStateRequest{Operations: ops}), "commit failed")
		assert.Empty(t, lease.revoked)
	})
}

func TestMultiLive(t *testing.T) {
	endpoints := os.Getenv("DAPR_TEST_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("DAPR_TEST_ETCD_ENDPOINTS not set")
	}

	for name, newStore := range map[string]func(logger.Logger) state.Store{
		"v1": NewEtcdStateStoreV1,
		"v2": NewEtcdStateStoreV2,
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(logger.NewLogger("test")).(*Etcd)
			require.NoError(t, s.Init(t.Context(), state.Metadata{Base: metadata.Base{Properties: map[string]string{
				"endpoints":     endpoints,
				"keyPrefixPath": "dapr-multi-test-" + uuid.NewString(),
			}}}))
			t.Cleanup(func() { s.Close() })
			testMultiLive(t, s)
		})
	}
}

func testMultiLive(t *testing.T, s *Etcd) {
	ctx := t.Context()
	get := func(t *testing.T, key string) *state.GetResponse {
		t.Helper()
		res, err := s.Get(ctx, &state.GetRequest{Key: key})
		require.NoError(t, err)
		return res
	}
	firstWrite := state.SetStateOption{Concurrency: state.FirstWrite}

	t.Run("stale etag fails the whole transaction", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "a", Value: "a1"}))
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "b", Value: "b1"}))
		etagB := get(t, "b").ETag

		err := s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "a2"},
			state.DeleteRequest{Key: "c"},
			state.SetRequest{Key: "b", Value: "b2", ETag: ptr.Of("1"), Options: firstWrite},
		}})
		requireETagKind(t, err, state.ETagMismatch)
		assert.JSONEq(t, `"a1"`, string(get(t, "a").Data))
		assert.Equal(t, etagB, get(t, "b").ETag)
	})

	t.Run("etag on a missing key fails", func(t *testing.T) {
		err := s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "missing1", Value: "x"},
			state.SetRequest{Key: "missing2", Value: "x", ETag: ptr.Of("3"), Options: firstWrite},
		}})
		requireETagKind(t, err, state.ETagMismatch)
		assert.Nil(t, get(t, "missing1").Data)
		assert.Nil(t, get(t, "missing2").Data)

		requireETagKind(t, s.Delete(ctx, &state.DeleteRequest{Key: "missing2", ETag: ptr.Of("5")}), state.ETagMismatch)
	})

	t.Run("stale delete fails", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "d", Value: "d1"}))
		err := s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "e", Value: "e1"},
			state.DeleteRequest{Key: "d", ETag: ptr.Of("1"), Options: state.DeleteStateOption{Concurrency: state.FirstWrite}},
		}})
		requireETagKind(t, err, state.ETagMismatch)
		assert.NotNil(t, get(t, "d").Data)
		assert.Nil(t, get(t, "e").Data)
	})

	t.Run("stale Set and Delete fail", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "sd", Value: "1"}))
		etag := get(t, "sd").ETag
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "sd", Value: "2", ETag: etag}))

		requireETagKind(t, s.Set(ctx, &state.SetRequest{Key: "sd", Value: "3", ETag: etag}), state.ETagMismatch)
		requireETagKind(t, s.Delete(ctx, &state.DeleteRequest{Key: "sd", ETag: etag}), state.ETagMismatch)
		assert.JSONEq(t, `"2"`, string(get(t, "sd").Data))
	})

	t.Run("first-write without etag on an existing key fails", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "fw", Value: "1"}))
		requireETagKind(t, s.Set(ctx, &state.SetRequest{Key: "fw", Value: "2", Options: firstWrite}), state.ETagMismatch)
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "fw-new", Value: "1", Options: firstWrite}))
	})

	t.Run("same key twice in one transaction", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "dup", Value: "1"}))
		etag := get(t, "dup").ETag
		requireETagKind(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "dup", Value: "2", ETag: etag},
			state.SetRequest{Key: "dup", Value: "3", ETag: etag},
		}}), state.ETagMismatch)
		assert.Equal(t, etag, get(t, "dup").ETag)

		require.NoError(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "dup", Value: "2", ETag: etag},
			state.DeleteRequest{Key: "dup"},
			state.SetRequest{Key: "dup", Value: "4", Options: firstWrite},
		}}))
		assert.JSONEq(t, `"4"`, string(get(t, "dup").Data))
	})

	t.Run("malformed etag is invalid", func(t *testing.T) {
		err := s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "a", Value: "x", ETag: ptr.Of("nope"), Options: firstWrite},
		}})
		requireETagKind(t, err, state.ETagInvalid)
	})

	t.Run("matching etags apply", func(t *testing.T) {
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "f", Value: "f1"}))
		require.NoError(t, s.Set(ctx, &state.SetRequest{Key: "g", Value: "g1"}))
		require.NoError(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "f", Value: "f2", ETag: get(t, "f").ETag, Options: firstWrite},
			state.DeleteRequest{Key: "g", ETag: get(t, "g").ETag},
			state.SetRequest{Key: "h", Value: "h1", Metadata: map[string]string{"ttlInSeconds": "100"}},
		}}))
		assert.JSONEq(t, `"f2"`, string(get(t, "f").Data))
		assert.Nil(t, get(t, "g").Data)
		assert.JSONEq(t, `"h1"`, string(get(t, "h").Data))
	})

	t.Run("unused leases are revoked and one ttl shares a lease", func(t *testing.T) {
		leases := func(t *testing.T) int {
			t.Helper()
			res, err := s.client.Leases(ctx)
			require.NoError(t, err)
			return len(res.Leases)
		}
		ttl := map[string]string{"ttlInSeconds": "3600"}
		before := leases(t)

		requireETagKind(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "l1", Value: "x", Metadata: ttl},
			state.SetRequest{Key: "l2", Value: "x", ETag: ptr.Of("1"), Metadata: ttl},
		}}), state.ETagMismatch)
		requireETagKind(t, s.Set(ctx, &state.SetRequest{Key: "l2", Value: "x", ETag: ptr.Of("1"), Metadata: ttl}), state.ETagMismatch)
		assert.Equal(t, before, leases(t))

		require.NoError(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "l1", Value: "x", Metadata: ttl},
			state.SetRequest{Key: "l2", Value: "x", Metadata: ttl},
			state.SetRequest{Key: "l3", Value: "x", Metadata: ttl},
		}}))
		assert.Equal(t, before+1, leases(t))
		require.NoError(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: []state.TransactionalStateOperation{
			state.DeleteRequest{Key: "l1"},
			state.DeleteRequest{Key: "l2"},
			state.DeleteRequest{Key: "l3"},
		}}))
	})

	t.Run("MultiMaxSize etag ops fit in one txn", func(t *testing.T) {
		n := s.MultiMaxSize()
		ops := make([]state.TransactionalStateOperation, n)
		for i := range n {
			ops[i] = state.SetRequest{Key: "max-" + strconv.Itoa(i), Value: i}
		}
		require.NoError(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: ops}))

		for i := range n {
			key := "max-" + strconv.Itoa(i)
			if i%2 == 0 {
				ops[i] = state.SetRequest{Key: key, Value: -i, ETag: get(t, key).ETag}
			} else {
				ops[i] = state.DeleteRequest{Key: key, ETag: get(t, key).ETag}
			}
		}
		require.NoError(t, s.Multi(ctx, &state.TransactionalStateRequest{Operations: ops}))
		assert.JSONEq(t, `-2`, string(get(t, "max-2").Data))
		assert.Nil(t, get(t, "max-1").Data)
	})
}
