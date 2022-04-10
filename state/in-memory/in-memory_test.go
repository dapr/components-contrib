package inmemory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/state"
)

func TestNewInMemoryStateStore(t *testing.T) {
	store := NewInMemoryStateStore(logger.NewLogger("test"))

	err := store.Init(state.Metadata{})
	assert.NoError(t, err)

	err = store.Ping()
	assert.NoError(t, err)

	assert.Equal(t, 0, len(store.Features()))

	err = store.Multi(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no support")

	_, err = store.Query(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no support")

	err = store.Close()
	assert.NoError(t, err)
}

func TestStoreByteArray(t *testing.T) {
	store := NewInMemoryStateStore(logger.NewLogger("test"))
	err := store.Init(state.Metadata{})
	assert.NoError(t, err)

	key := "key-abcdefg"
	value := []byte("value-abcdefg")

	// step1: save
	req := &state.SetRequest{
		Key:   key,
		Value: value,
	}
	err = store.Set(req)
	assert.NoError(t, err)
	// save is async so wait for a moment
	time.Sleep(1 * time.Millisecond)

	// step2: get
	req2 := &state.GetRequest{
		Key: key,
	}
	response, err := store.Get(req2)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.NotNil(t, response.Data)
	assert.Equal(t, value, response.Data)

	// step3: delete
	req3 := &state.DeleteRequest{
		Key: key,
	}
	err = store.Delete(req3)
	assert.NoError(t, err)

	// step4: get
	req2 = &state.GetRequest{
		Key: key,
	}
	response, err = store.Get(req2)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Nil(t, response.Data)
}

func TestStoreNonByteArray(t *testing.T) {
	store := NewInMemoryStateStore(logger.NewLogger("test"))
	err := store.Init(state.Metadata{})
	assert.NoError(t, err)

	key := "key-abcdefg"
	value := 1234

	// step1: save with non byte[]
	req := &state.SetRequest{
		Key:   key,
		Value: value,
	}
	err = store.Set(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only support byte array")

	err = store.Close()
	assert.NoError(t, err)

	// step2: save after closed
	req = &state.SetRequest{
		Key:   key,
		Value: []byte{},
	}
	err = store.Set(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail to save to in-memory cache")
}
