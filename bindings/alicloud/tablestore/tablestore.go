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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	tableName   = "tableName"
	columnToGet = "columnToGet"
	primaryKeys = "primaryKeys"

	invokeStartTimeKey = "start-time"
	invokeEndTimeKey   = "end-time"
	invokeDurationKey  = "duration"
)

type tablestoreMetadata struct {
	Endpoint     string `json:"endpoint" mapstructure:"endpoint"`
	AccessKeyID  string `json:"accessKeyID" mapstructure:"accessKeyID"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey"`
	InstanceName string `json:"instanceName" mapstructure:"instanceName"`
	TableName    string `json:"tableName" mapstructure:"tableName"`
}

type AliCloudTableStore struct {
	logger   logger.Logger
	client   *tablestore.TableStoreClient
	metadata tablestoreMetadata
}

func NewAliCloudTableStore(log logger.Logger) bindings.OutputBinding {
	return &AliCloudTableStore{
		logger: log,
		client: nil,
	}
}

func (s *AliCloudTableStore) Init(_ context.Context, metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}

	s.metadata = *m
	s.client = tablestore.NewClient(m.Endpoint, m.InstanceName, m.AccessKeyID, m.AccessKey)

	return nil
}

func (s *AliCloudTableStore) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, errors.New("invoke request required")
	}

	startTime := time.Now()
	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			invokeStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}

	switch req.Operation {
	case bindings.GetOperation:
		err := s.get(req, resp)
		if err != nil {
			return nil, err
		}
	case bindings.ListOperation:
		err := s.list(req, resp)
		if err != nil {
			return nil, err
		}
	case bindings.CreateOperation:
		err := s.create(req, resp)
		if err != nil {
			return nil, err
		}
	case bindings.DeleteOperation:
		err := s.delete(req, resp)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid operation type: %s. Expected %s, %s, %s, or %s",
			req.Operation, bindings.GetOperation, bindings.ListOperation, bindings.CreateOperation, bindings.DeleteOperation)
	}

	endTime := time.Now()
	resp.Metadata[invokeEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[invokeDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}

func (s *AliCloudTableStore) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.DeleteOperation, bindings.GetOperation, bindings.ListOperation}
}

func (s *AliCloudTableStore) parseMetadata(metadata bindings.Metadata) (*tablestoreMetadata, error) {
	m := tablestoreMetadata{}
	err := kitmd.DecodeMetadata(metadata.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AliCloudTableStore) get(req *bindings.InvokeRequest, resp *bindings.InvokeResponse) error {
	columns := strings.Split(req.Metadata[columnToGet], ",")
	pkNames := strings.Split(req.Metadata[primaryKeys], ",")
	pks := make([]*tablestore.PrimaryKeyColumn, len(pkNames))

	data := make(map[string]interface{})
	err := json.Unmarshal(req.Data, &data)
	if err != nil {
		return err
	}

	for idx, pkName := range pkNames {
		pks[idx] = &tablestore.PrimaryKeyColumn{
			ColumnName: pkName,
			Value:      data[pkName],
		}
	}

	criteria := &tablestore.SingleRowQueryCriteria{
		TableName:    s.getTableName(req.Metadata),
		PrimaryKey:   &tablestore.PrimaryKey{PrimaryKeys: pks},
		ColumnsToGet: columns,
		MaxVersion:   1,
	}
	getRowReq := &tablestore.GetRowRequest{
		SingleRowQueryCriteria: criteria,
	}
	getRowResp, err := s.client.GetRow(getRowReq)
	if err != nil {
		return err
	}

	ret, err := s.unmarshal(getRowResp.PrimaryKey.PrimaryKeys, getRowResp.Columns)
	if err != nil {
		return err
	}

	if ret == nil {
		resp.Data = nil

		return nil
	}

	resp.Data, err = json.Marshal(ret)

	return err
}

func (s *AliCloudTableStore) list(req *bindings.InvokeRequest, resp *bindings.InvokeResponse) error {
	columns := strings.Split(req.Metadata[columnToGet], ",")
	pkNames := strings.Split(req.Metadata[primaryKeys], ",")

	var data []map[string]interface{}
	err := json.Unmarshal(req.Data, &data)
	if err != nil {
		return err
	}

	criteria := &tablestore.MultiRowQueryCriteria{
		TableName:    s.getTableName(req.Metadata),
		ColumnsToGet: columns,
		MaxVersion:   1,
	}

	for _, item := range data {
		pk := &tablestore.PrimaryKey{}
		for _, pkName := range pkNames {
			pk.AddPrimaryKeyColumn(pkName, item[pkName])
		}
		criteria.AddRow(pk)
	}

	getRowRequest := &tablestore.BatchGetRowRequest{}
	getRowRequest.MultiRowQueryCriteria = append(getRowRequest.MultiRowQueryCriteria, criteria)
	getRowResp, err := s.client.BatchGetRow(getRowRequest)
	if err != nil {
		return err
	}

	var ret []interface{}

	for _, criteria := range getRowRequest.MultiRowQueryCriteria {
		for _, row := range getRowResp.TableToRowsResult[criteria.TableName] {
			rowData, rowErr := s.unmarshal(row.PrimaryKey.PrimaryKeys, row.Columns)
			if rowErr != nil {
				return rowErr
			}
			ret = append(ret, rowData)
		}
	}

	resp.Data, err = json.Marshal(ret)

	return err
}

func (s *AliCloudTableStore) create(req *bindings.InvokeRequest, resp *bindings.InvokeResponse) error {
	data := make(map[string]interface{})
	err := json.Unmarshal(req.Data, &data)
	if err != nil {
		return err
	}
	pkNames := strings.Split(req.Metadata[primaryKeys], ",")
	pks := make([]*tablestore.PrimaryKeyColumn, len(pkNames))
	columns := make([]tablestore.AttributeColumn, len(data)-len(pkNames))

	for idx, pk := range pkNames {
		pks[idx] = &tablestore.PrimaryKeyColumn{
			ColumnName: pk,
			Value:      data[pk],
		}
	}

	idx := 0
	for key, val := range data {
		if !contains(pkNames, key) {
			columns[idx] = tablestore.AttributeColumn{
				ColumnName: key,
				Value:      val,
			}
			idx++
		}
	}

	change := tablestore.PutRowChange{
		TableName:     s.getTableName(req.Metadata),
		PrimaryKey:    &tablestore.PrimaryKey{PrimaryKeys: pks},
		Columns:       columns,
		ReturnType:    tablestore.ReturnType_RT_NONE, //nolint:nosnakecase
		TransactionId: nil,
	}

	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE) //nolint:nosnakecase

	putRequest := &tablestore.PutRowRequest{
		PutRowChange: &change,
	}

	_, err = s.client.PutRow(putRequest)
	if err != nil {
		return err
	}

	return nil
}

func (s *AliCloudTableStore) delete(req *bindings.InvokeRequest, resp *bindings.InvokeResponse) error {
	pkNams := strings.Split(req.Metadata[primaryKeys], ",")
	pks := make([]*tablestore.PrimaryKeyColumn, len(pkNams))
	data := make(map[string]interface{})
	err := json.Unmarshal(req.Data, &data)
	if err != nil {
		return err
	}

	for idx, pkName := range pkNams {
		pks[idx] = &tablestore.PrimaryKeyColumn{
			ColumnName: pkName,
			Value:      data[pkName],
		}
	}

	change := &tablestore.DeleteRowChange{
		TableName:  s.getTableName(req.Metadata),
		PrimaryKey: &tablestore.PrimaryKey{PrimaryKeys: pks},
	}
	change.SetCondition(tablestore.RowExistenceExpectation_IGNORE) //nolint:nosnakecase
	deleteReq := &tablestore.DeleteRowRequest{DeleteRowChange: change}
	_, err = s.client.DeleteRow(deleteReq)
	if err != nil {
		return err
	}

	return nil
}

func (s *AliCloudTableStore) unmarshal(pks []*tablestore.PrimaryKeyColumn, columns []*tablestore.AttributeColumn) (map[string]interface{}, error) {
	if pks == nil && columns == nil {
		return nil, nil
	}

	data := make(map[string]interface{})

	for _, pk := range pks {
		data[pk.ColumnName] = pk.Value
	}

	for _, column := range columns {
		data[column.ColumnName] = column.Value
	}

	return data, nil
}

func (s *AliCloudTableStore) getTableName(metadata map[string]string) string {
	name := metadata[tableName]
	if name == "" {
		name = s.metadata.TableName
	}

	return name
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}

	return false
}

// GetComponentMetadata returns the metadata of the component.
func (s *AliCloudTableStore) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := tablestoreMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}

func (s *AliCloudTableStore) Close() error {
	return nil
}
