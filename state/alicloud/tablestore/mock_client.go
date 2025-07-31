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
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type mockClient struct {
	tablestore.TableStoreClient

	data map[string][]byte
	mu   sync.RWMutex
}

func (m *mockClient) DeleteRow(request *tablestore.DeleteRowRequest) (*tablestore.DeleteRowResponse, error) {
	var key string
	for _, col := range request.DeleteRowChange.PrimaryKey.PrimaryKeys {
		if col.ColumnName == stateKey {
			key = col.Value.(string)

			break
		}
	}

	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()

	return nil, nil
}

func (m *mockClient) GetRow(request *tablestore.GetRowRequest) (*tablestore.GetRowResponse, error) {
	var key string
	for _, col := range request.SingleRowQueryCriteria.PrimaryKey.PrimaryKeys {
		if col.ColumnName == stateKey {
			key = col.Value.(string)

			break
		}
	}

	m.mu.RLock()
	val := m.data[key]
	m.mu.RUnlock()

	resp := &tablestore.GetRowResponse{
		Columns: []*tablestore.AttributeColumn{{
			ColumnName: stateValue,
			Value:      val,
		}},
	}

	return resp, nil
}

func (m *mockClient) UpdateRow(req *tablestore.UpdateRowRequest) (*tablestore.UpdateRowResponse, error) {
	change := req.UpdateRowChange

	var val []byte
	var key string

	for _, col := range change.PrimaryKey.PrimaryKeys {
		if col.ColumnName == stateKey {
			key = col.Value.(string)

			break
		}
	}

	for _, col := range change.Columns {
		if col.ColumnName == stateValue {
			buf := &bytes.Buffer{}
			binary.Write(buf, binary.BigEndian, col.Value)
			val = buf.Bytes()

			break
		}
	}

	m.mu.Lock()
	m.data[key] = val
	m.mu.Unlock()

	return nil, nil
}

func (m *mockClient) BatchGetRow(request *tablestore.BatchGetRowRequest) (*tablestore.BatchGetRowResponse, error) {
	resp := &tablestore.BatchGetRowResponse{
		TableToRowsResult: map[string][]tablestore.RowResult{},
	}

	m.mu.RLock()
	for _, criteria := range request.MultiRowQueryCriteria {
		tableRes := resp.TableToRowsResult[criteria.TableName]
		if tableRes == nil {
			tableRes = []tablestore.RowResult{}
		}
		for _, keys := range criteria.PrimaryKey {
			for _, key := range keys.PrimaryKeys {
				if key.ColumnName == stateKey {
					pk := key.Value.(string)

					if m.data[pk] == nil {
						continue
					}

					value := m.data[key.Value.(string)]
					tableRes = append(tableRes, tablestore.RowResult{
						TableName: criteria.TableName,
						Columns: []*tablestore.AttributeColumn{
							{
								ColumnName: stateValue,
								Value:      value,
							},
						},
						PrimaryKey: tablestore.PrimaryKey{
							PrimaryKeys: []*tablestore.PrimaryKeyColumn{
								{
									ColumnName: stateKey,
									Value:      key.Value,
								},
							},
						},
					})
					resp.TableToRowsResult[criteria.TableName] = tableRes

					break
				}
			}
		}
	}
	m.mu.RUnlock()

	return resp, nil
}

func (m *mockClient) BatchWriteRow(request *tablestore.BatchWriteRowRequest) (*tablestore.BatchWriteRowResponse, error) {
	resp := &tablestore.BatchWriteRowResponse{}
	m.mu.Lock()
	for _, changes := range request.RowChangesGroupByTable {
		for _, change := range changes {
			switch inst := change.(type) {
			case *tablestore.UpdateRowChange:
				var pk string
				for _, col := range inst.PrimaryKey.PrimaryKeys {
					if col.ColumnName == stateKey {
						pk = col.Value.(string)

						break
					}
				}

				for _, col := range inst.Columns {
					if col.ColumnName == stateValue {
						buf := &bytes.Buffer{}
						binary.Write(buf, binary.BigEndian, col.Value)
						m.data[pk] = buf.Bytes()
					}
				}

			case *tablestore.DeleteRowChange:
				for _, col := range inst.PrimaryKey.PrimaryKeys {
					if col.ColumnName == stateKey {
						delete(m.data, col.Value.(string))

						break
					}
				}
			}
		}
	}
	m.mu.Unlock()

	return resp, nil
}
