package client

import (
	"context"
	"testing"

	v1 "github.com/dapr/dapr/pkg/proto/common/v1"
	"github.com/stretchr/testify/assert"
)

type _testStructwithText struct {
	Key1, Key2 string
}

type _testStructwithTextandNumbers struct {
	Key1 string
	Key2 int
}

type _testStructwithSlices struct {
	Key1 []string
	Key2 []int
}

func TestInvokeMethodWithContent(t *testing.T) {
	ctx := context.Background()
	data := "ping"

	t.Run("with content", func(t *testing.T) {
		content := &DataContent{
			ContentType: "text/plain",
			Data:        []byte(data),
		}
		resp, err := testClient.InvokeMethodWithContent(ctx, "test", "fn", "post", content)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, string(resp), data)
	})

	t.Run("with content, method contains querystring", func(t *testing.T) {
		content := &DataContent{
			ContentType: "text/plain",
			Data:        []byte(data),
		}
		resp, err := testClient.InvokeMethodWithContent(ctx, "test", "fn?foo=bar&url=http://dapr.io", "get", content)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, string(resp), data)
	})

	t.Run("without content", func(t *testing.T) {
		resp, err := testClient.InvokeMethod(ctx, "test", "fn", "get")
		assert.Nil(t, err)
		assert.Nil(t, resp)
	})

	t.Run("without service ID", func(t *testing.T) {
		_, err := testClient.InvokeMethod(ctx, "", "fn", "get")
		assert.NotNil(t, err)
	})
	t.Run("without method", func(t *testing.T) {
		_, err := testClient.InvokeMethod(ctx, "test", "", "get")
		assert.NotNil(t, err)
	})
	t.Run("without verb", func(t *testing.T) {
		_, err := testClient.InvokeMethod(ctx, "test", "fn", "")
		assert.NotNil(t, err)
	})
	t.Run("from struct with text", func(t *testing.T) {
		testdata := _testCustomContentwithText{
			Key1: "value1",
			Key2: "value2",
		}
		_, err := testClient.InvokeMethodWithCustomContent(ctx, "test", "fn", "post", "text/plain", testdata)
		assert.Nil(t, err)
	})

	t.Run("from struct with text and numbers", func(t *testing.T) {
		testdata := _testCustomContentwithTextandNumbers{
			Key1: "value1",
			Key2: 2500,
		}
		_, err := testClient.InvokeMethodWithCustomContent(ctx, "test", "fn", "post", "text/plain", testdata)
		assert.Nil(t, err)
	})

	t.Run("from struct with slices", func(t *testing.T) {
		testdata := _testCustomContentwithSlices{
			Key1: []string{"value1", "value2", "value3"},
			Key2: []int{25, 40, 600},
		}
		_, err := testClient.InvokeMethodWithCustomContent(ctx, "test", "fn", "post", "text/plain", testdata)
		assert.Nil(t, err)
	})
}

func TestVerbParsing(t *testing.T) {
	t.Run("valid lower case", func(t *testing.T) {
		v := queryAndVerbToHTTPExtension("", "post")
		assert.NotNil(t, v)
		assert.Equal(t, v1.HTTPExtension_POST, v.Verb)
		assert.Len(t, v.Querystring, 0)
	})

	t.Run("valid upper case", func(t *testing.T) {
		v := queryAndVerbToHTTPExtension("", "GET")
		assert.NotNil(t, v)
		assert.Equal(t, v1.HTTPExtension_GET, v.Verb)
	})

	t.Run("invalid verb", func(t *testing.T) {
		v := queryAndVerbToHTTPExtension("", "BAD")
		assert.NotNil(t, v)
		assert.Equal(t, v1.HTTPExtension_NONE, v.Verb)
	})

	t.Run("valid query", func(t *testing.T) {
		v := queryAndVerbToHTTPExtension("foo=bar&url=http://dapr.io", "post")
		assert.NotNil(t, v)
		assert.Equal(t, v1.HTTPExtension_POST, v.Verb)
		assert.Equal(t, "foo=bar&url=http://dapr.io", v.Querystring)
	})
}

func TestExtractMethodAndQuery(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name       string
		args       args
		wantMethod string
		wantQuery  string
	}{
		{
			"pure uri",
			args{name: "method"},
			"method",
			"",
		},
		{
			"root route method",
			args{name: "/"},
			"/",
			"",
		},
		{
			"uri with one query",
			args{name: "method?foo=bar"},
			"method",
			"foo=bar",
		},
		{
			"uri with two query",
			args{name: "method?foo=bar&url=http://dapr.io"},
			"method",
			"foo=bar&url=http://dapr.io",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMethod, gotQuery := extractMethodAndQuery(tt.args.name)
			if gotMethod != tt.wantMethod {
				t.Errorf("extractMethodAndQuery() gotMethod = %v, want %v", gotMethod, tt.wantMethod)
			}
			if gotQuery != tt.wantQuery {
				t.Errorf("extractMethodAndQuery() gotQuery = %v, want %v", gotQuery, tt.wantQuery)
			}
		})
	}
}
