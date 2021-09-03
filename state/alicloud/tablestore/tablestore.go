// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package tablestore

import (
	"encoding/json"

	"github.com/agrea/ptr"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	stateKeyName         = "stateKey"
	stateValueColumnName = "stateValue"
	stateETagColumnName  = "sateEtag"
)

type AliCloudTableStore struct {
	logger   logger.Logger
	client   *tablestore.TableStoreClient
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

func NewAliCloudTableStore(logger logger.Logger) *AliCloudTableStore {
	return &AliCloudTableStore{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
}

func (s *AliCloudTableStore) Init(metadata state.Metadata) error {
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

func (s *AliCloudTableStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.PrimaryKey = s.getPK(req.Key)
	criteria.TableName = s.metadata.TableName
	criteria.MaxVersion = 1

	rowGetReq := new(tablestore.GetRowRequest)
	rowGetReq.SingleRowQueryCriteria = criteria

	resp, err := s.client.GetRow(rowGetReq)
	if err != nil {
		return nil, err
	}

	getResp := s.toGetResp(resp.Columns)

	return getResp, nil
}

func (s *AliCloudTableStore) toGetResp(columns []*tablestore.AttributeColumn) *state.GetResponse {
	getResp := &state.GetResponse{}

	for _, column := range columns {
		if column.ColumnName == stateValueColumnName {
			getResp.Data = column.Value.([]byte)
		} else if column.ColumnName == stateETagColumnName {
			getResp.ETag = ptr.String(column.Value.(string))
		}
	}
	return getResp
}

func (s *AliCloudTableStore) BulkGet(reqs []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// "len == 0": empty request, directly return empty response
	if len(reqs) == 0 {
		return true, []state.BulkGetResponse{}, nil
	}

	mqCriteria := new(tablestore.MultiRowQueryCriteria)
	mqCriteria.TableName = s.metadata.TableName
	mqCriteria.MaxVersion = 1

	for _, req := range reqs {
		mqCriteria.AddRow(s.getPK(req.Key))
	}

	batchGetReq := new(tablestore.BatchGetRowRequest)
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResp, err := s.client.BatchGetRow(batchGetReq)
	responseList := make([]state.BulkGetResponse, 0, 10)
	if err != nil {
		return false, nil, err
	}

	for _, row := range batchGetResp.TableToRowsResult[mqCriteria.TableName] {
		resp := s.toGetResp(row.Columns)

		responseList = append(responseList, state.BulkGetResponse{
			Data: resp.Data,
			ETag: resp.ETag,
			Key:  row.PrimaryKey.PrimaryKeys[0].Value.(string),
		})
	}

	return true, responseList, nil
}

func (s *AliCloudTableStore) Set(req *state.SetRequest) error {
	change := s.toUpdateRowChange(req)

	request := new(tablestore.UpdateRowRequest)
	request.UpdateRowChange = change

	_, err := s.client.UpdateRow(request)
	if err != nil {
		return err
	}

	return nil
}

func (s *AliCloudTableStore) toUpdateRowChange(req *state.SetRequest) *tablestore.UpdateRowChange {
	change := new(tablestore.UpdateRowChange)
	change.PrimaryKey = s.getPK(req.Key)
	change.TableName = s.metadata.TableName

	value, _ := utils.Marshal(req.Value, json.Marshal)

	change.PutColumn(stateValueColumnName, value)

	if req.ETag != nil {
		change.PutColumn(stateETagColumnName, *req.ETag)
	}

	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	return change
}

func (s *AliCloudTableStore) Delete(req *state.DeleteRequest) error {
	change := s.getDeleteRowChange(req)

	deleteRowReq := new(tablestore.DeleteRowRequest)
	deleteRowReq.DeleteRowChange = change

	_, err := s.client.DeleteRow(deleteRowReq)
	if err != nil {
		return err
	}

	return nil
}

func (s *AliCloudTableStore) getDeleteRowChange(req *state.DeleteRequest) *tablestore.DeleteRowChange {
	change := new(tablestore.DeleteRowChange)
	change.PrimaryKey = s.getPK(req.Key)
	change.TableName = s.metadata.TableName
	change.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	return change
}

func (s *AliCloudTableStore) BulkSet(reqs []state.SetRequest) error {
	return s.batchWrite(reqs, nil)
}

func (s *AliCloudTableStore) BulkDelete(reqs []state.DeleteRequest) error {
	return s.batchWrite(nil, reqs)
}

func (s *AliCloudTableStore) batchWrite(setReqs []state.SetRequest, deleteReqs []state.DeleteRequest) error {
	bathReq := new(tablestore.BatchWriteRowRequest)

	for i := range setReqs {
		bathReq.AddRowChange(s.toUpdateRowChange(&setReqs[i]))
	}

	for i := range deleteReqs {
		bathReq.AddRowChange(s.getDeleteRowChange(&deleteReqs[i]))
	}

	bathReq.IsAtomic = true
	_, err := s.client.BatchWriteRow(bathReq)
	if err != nil {
		return err
	}

	return nil
}

func (s *AliCloudTableStore) Ping() error {
	return nil
}

func (s *AliCloudTableStore) parse(metadata state.Metadata) (*tablestoreMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m tablestoreMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AliCloudTableStore) getPK(key string) *tablestore.PrimaryKey {
	deletePk := new(tablestore.PrimaryKey)
	deletePk.AddPrimaryKeyColumn(stateKeyName, key)
	return deletePk
}
