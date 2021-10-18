package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type _testCustomContentwithText struct {
	Key1, Key2 string
}

type _testCustomContentwithTextandNumbers struct {
	Key1 string
	Key2 int
}

type _testCustomContentwithSlices struct {
	Key1 []string
	Key2 []int
}

// go test -timeout 30s ./client -count 1 -run ^TestPublishEvent$
func TestPublishEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("with data", func(t *testing.T) {
		err := testClient.PublishEvent(ctx, "messages", "test", []byte("ping"))
		assert.Nil(t, err)
	})

	t.Run("without data", func(t *testing.T) {
		err := testClient.PublishEvent(ctx, "messages", "test", nil)
		assert.Nil(t, err)
	})

	t.Run("with empty topic name", func(t *testing.T) {
		err := testClient.PublishEvent(ctx, "messages", "", []byte("ping"))
		assert.NotNil(t, err)
	})

	t.Run("from struct with text", func(t *testing.T) {
		testdata := _testStructwithText{
			Key1: "value1",
			Key2: "value2",
		}
		err := testClient.PublishEventfromCustomContent(ctx, "messages", "test", testdata)
		assert.Nil(t, err)
	})

	t.Run("from struct with text and numbers", func(t *testing.T) {
		testdata := _testStructwithTextandNumbers{
			Key1: "value1",
			Key2: 2500,
		}
		err := testClient.PublishEventfromCustomContent(ctx, "messages", "test", testdata)
		assert.Nil(t, err)
	})

	t.Run("from struct with slices", func(t *testing.T) {
		testdata := _testStructwithSlices{
			Key1: []string{"value1", "value2", "value3"},
			Key2: []int{25, 40, 600},
		}
		err := testClient.PublishEventfromCustomContent(ctx, "messages", "test", testdata)
		assert.Nil(t, err)
	})
}
