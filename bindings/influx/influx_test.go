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

package influx

import (
	"context"
	"testing"

	"github.com/dapr/components-contrib/bindings"

	"github.com/dapr/kit/logger"
	"github.com/golang/mock/gomock"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"Url": "a", "Token": "a", "Org": "a", "Bucket": "a"}
	influx := Influx{logger: logger.NewLogger("test")}
	im, err := influx.getInfluxMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", im.URL)
	assert.Equal(t, "a", im.Token)
	assert.Equal(t, "a", im.Org)
	assert.Equal(t, "a", im.Bucket)
}

func TestOperations(t *testing.T) {
	opers := (*Influx)(nil).Operations()
	assert.Equal(t, []bindings.OperationKind{
		bindings.CreateOperation,
		queryOperation,
	}, opers)
}

func TestInflux_Init(t *testing.T) {
	influx := NewInflux(logger.NewLogger("test"))
	assert.Nil(t, influx.queryAPI)
	assert.Nil(t, influx.writeAPI)
	assert.Nil(t, influx.metadata)
	assert.Nil(t, influx.client)

	m := bindings.Metadata{Properties: map[string]string{"Url": "a", "Token": "a", "Org": "a", "Bucket": "a"}}
	err := influx.Init(m)
	assert.Nil(t, err)

	assert.NotNil(t, influx.queryAPI)
	assert.NotNil(t, influx.writeAPI)
	assert.NotNil(t, influx.metadata)
	assert.NotNil(t, influx.client)
}

func TestInflux_Invoke_BindingCreateOperation(t *testing.T) {
	tests := []struct {
		name    string
		request *bindings.InvokeRequest
		want    struct {
			resp *bindings.InvokeResponse
			err  error
		}
	}{
		{"invoke by invalid request metadata", &bindings.InvokeRequest{Operation: bindings.CreateOperation}, struct {
			resp *bindings.InvokeResponse
			err  error
		}{resp: nil, err: ErrInvalidRequestData}},
		{"invoke valid request metadata", &bindings.InvokeRequest{
			Data:      []byte(`{"measurement":"a", "tags":"a", "values":"a"}`),
			Operation: bindings.CreateOperation,
		}, struct {
			resp *bindings.InvokeResponse
			err  error
		}{resp: nil, err: nil}},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := NewMockWriteAPIBlocking(ctrl)
	w.EXPECT().WriteRecord(gomock.Eq(context.Background()), gomock.Eq("a,a a")).Return(nil)
	influx := &Influx{
		writeAPI: w,
	}
	for _, test := range tests {
		resp, err := influx.Invoke(test.request)
		assert.Equal(t, test.want.resp, resp)
		assert.Equal(t, test.want.err, err)
	}

}

func TestInflux_Invoke_BindingInvalidOperation(t *testing.T) {
	tests := []struct {
		name    string
		request *bindings.InvokeRequest
		want    struct {
			resp *bindings.InvokeResponse
			err  error
		}
	}{
		{"invoke by invalid request metadata", &bindings.InvokeRequest{Operation: bindings.CreateOperation}, struct {
			resp *bindings.InvokeResponse
			err  error
		}{resp: nil, err: ErrInvalidRequestData}},
	}

	for _, test := range tests {
		resp, err := (*Influx)(nil).Invoke(test.request)
		assert.Equal(t, test.want.resp, resp)
		assert.Equal(t, test.want.err, err)
	}

}

func TestInflux_Invoke_BindingQueryOperation(t *testing.T) {
	tests := []struct {
		name    string
		request *bindings.InvokeRequest
		want    struct {
			resp *bindings.InvokeResponse
			err  error
		}
	}{
		{"invoke by missing request metadata", &bindings.InvokeRequest{Operation: "query"}, struct {
			resp *bindings.InvokeResponse
			err  error
		}{resp: nil, err: ErrMetadataMissing}},
		{"invoke by invalid request metadata", &bindings.InvokeRequest{Operation: "query", Metadata: map[string]string{"notRaw": "test"}}, struct {
			resp *bindings.InvokeResponse
			err  error
		}{resp: nil, err: ErrMetadataRawNotFound}},
		{"invoke valid request metadata", &bindings.InvokeRequest{
			Metadata:  map[string]string{"raw": "a"},
			Operation: "query",
		}, struct {
			resp *bindings.InvokeResponse
			err  error
		}{resp: &bindings.InvokeResponse{Metadata: map[string]string{respOperatorKey: "query", rawQueryKey: "a"}, Data: []byte("ok")}, err: nil}},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	q := NewMockQueryAPI(ctrl)
	q.EXPECT().QueryRaw(gomock.Eq(context.Background()), gomock.Eq("a"), gomock.Eq(influxdb2.DefaultDialect())).Return("ok", nil)
	influx := &Influx{
		queryAPI: q,
		logger:   logger.NewLogger("test"),
	}
	for _, test := range tests {
		resp, err := influx.Invoke(test.request)
		assert.Equal(t, test.want.resp, resp)
		assert.Equal(t, test.want.err, err)
	}
}
