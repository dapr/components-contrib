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

package tablestore

import (
	"context"
	"reflect"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	stateKey   = "stateKey"
	stateValue = "stateValue"
	sateEtag   = "sateEtag"
)

type AliCloudTableStore struct {
	state.BulkStore

	logger   logger.Logger
	client   tablestore.TableStoreApi
	metadata tablestoreMetadata
	features []state.Feature
}

type tablestoreMetadata struct {
	Endpoint     string `json:"endpoint"`
	AccessKeyID  string `json:"accessKeyID"`
	AccessKey    string `json:"accessKey"`
	InstanceName string `json:"instanceName"`
	TableName    string `json:"tableName"`
}

func NewAliCloudTableStore(logger logger.Logger) state.Store {
	s := &AliCloudTableStore{
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (s *AliCloudTableStore) Init(_ context.Context, metadata state.Metadata) error {
	m, err := s.parse(metadata)
	if err != nil {
		return err
	}

	s.metadata = *m
	s.client = tablestore.NewClient(m.Endpoint, m.InstanceName, m.AccessKeyID, m.AccessKey)

	return nil
}

func (s *AliCloudTableStore) Features() []state.Feature {
	return s.features
}

func (s *AliCloudTableStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	criteria := &tablestore.SingleRowQueryCriteria{
		PrimaryKey: s.primaryKey(req.Key),
		TableName:  s.metadata.TableName,
		MaxVersion: 1,
	}

	rowGetReq := &tablestore.GetRowRequest{
		SingleRowQueryCriteria: criteria,
	}

	resp, err := s.client.GetRow(rowGetReq)
	if err != nil {
		return nil, err
	}

	getResp := s.getResp(resp.Columns)

	return getResp, nil
}

func (s *AliCloudTableStore) getResp(columns []*tablestore.AttributeColumn) *state.GetResponse {
	getResp := &state.GetResponse{}

	for _, column := range columns {
		if column.ColumnName == stateValue {
			getResp.Data = unmarshal(column.Value)
		} else if column.ColumnName == sateEtag {
			getResp.ETag = ptr.Of(column.Value.(string))
		}
	}

	return getResp
}

// Options are ignored because this component requests all values in a single operation.
func (s *AliCloudTableStore) BulkGet(ctx context.Context, reqs []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	// "len == 0": empty request, directly return empty response
	if len(reqs) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	mqCriteria := &tablestore.MultiRowQueryCriteria{
		TableName:  s.metadata.TableName,
		MaxVersion: 1,
	}

	for _, req := range reqs {
		mqCriteria.AddRow(s.primaryKey(req.Key))
	}

	batchGetReq := &tablestore.BatchGetRowRequest{
		MultiRowQueryCriteria: []*tablestore.MultiRowQueryCriteria{
			mqCriteria,
		},
	}
	batchGetResp, err := s.client.BatchGetRow(batchGetReq)
	if err != nil {
		return nil, err
	}

	responseList := make([]state.BulkGetResponse, len(batchGetResp.TableToRowsResult[mqCriteria.TableName]))
	for i, row := range batchGetResp.TableToRowsResult[mqCriteria.TableName] {
		resp := s.getResp(row.Columns)

		responseList[i] = state.BulkGetResponse{
			Data: resp.Data,
			ETag: resp.ETag,
			Key:  row.PrimaryKey.PrimaryKeys[0].Value.(string),
		}
	}

	return responseList, nil
}

func (s *AliCloudTableStore) Set(ctx context.Context, req *state.SetRequest) error {
	change := s.updateRowChange(req)

	request := &tablestore.UpdateRowRequest{
		UpdateRowChange: change,
	}

	_, err := s.client.UpdateRow(request)

	return err
}

func (s *AliCloudTableStore) updateRowChange(req *state.SetRequest) *tablestore.UpdateRowChange {
	change := &tablestore.UpdateRowChange{
		PrimaryKey: s.primaryKey(req.Key),
		TableName:  s.metadata.TableName,
	}

	value, _ := marshal(req.Value)
	change.PutColumn(stateValue, value)

	if req.HasETag() {
		change.PutColumn(sateEtag, *req.ETag)
	}

	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE) //nolint:nosnakecase

	return change
}

func marshal(value interface{}) ([]byte, error) {
	v, _ := jsoniter.MarshalToString(value)

	return []byte(v), nil
}

func unmarshal(val interface{}) []byte {
	var output string

	jsoniter.UnmarshalFromString(string(val.([]byte)), &output)

	return []byte(output)
}

func (s *AliCloudTableStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	change := s.deleteRowChange(req)

	deleteRowReq := &tablestore.DeleteRowRequest{
		DeleteRowChange: change,
	}

	_, err := s.client.DeleteRow(deleteRowReq)

	return err
}

func (s *AliCloudTableStore) deleteRowChange(req *state.DeleteRequest) *tablestore.DeleteRowChange {
	change := &tablestore.DeleteRowChange{
		PrimaryKey: s.primaryKey(req.Key),
		TableName:  s.metadata.TableName,
	}
	change.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST) //nolint:nosnakecase

	return change
}

func (s *AliCloudTableStore) parse(meta state.Metadata) (*tablestoreMetadata, error) {
	var m tablestoreMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	return &m, err
}

func (s *AliCloudTableStore) primaryKey(key string) *tablestore.PrimaryKey {
	pk := &tablestore.PrimaryKey{}
	pk.AddPrimaryKeyColumn(stateKey, key)

	return pk
}

func (s *AliCloudTableStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := tablestoreMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (s *AliCloudTableStore) Close() error {
	return nil
}
