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

package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/dapr/components-contrib/common/component/aws/opensearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/logger"
)

// OpenSearch stores dense vectors in OpenSearch knn_vector fields.
type OpenSearch struct {
	log    logger.Logger
	mu     sync.RWMutex
	client *common.Client
	closed bool
}

func NewOpenSearch(log logger.Logger) vector.Vector {
	return &OpenSearch{log: log}
}

func (o *OpenSearch) Init(ctx context.Context, meta vector.Metadata) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return status.Error(codes.FailedPrecondition, "the opensearch vector component is closed")
	}
	if o.client != nil {
		return nil
	}
	client, err := common.NewClient(ctx, meta.Properties, o.log)
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

func (o *OpenSearch) ready() (*common.Client, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed || o.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "the opensearch vector component is not initialized or is closed")
	}
	return o.client, nil
}

func (o *OpenSearch) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	if o.client != nil {
		return o.client.Close()
	}
	return nil
}

func (o *OpenSearch) GetComponentMetadata() (info metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(common.Metadata{}), &info, metadata.VectorType)
	delete(info, "Logger")
	delete(info, "Properties")
	for name, field := range info {
		if name != "endpoint" && name != "timeout" {
			field.Ignored = true
			info[name] = field
		}
	}
	return info
}

type collectionInfo struct {
	Kind       string                `json:"dapr_kind"`
	Dimensions uint32                `json:"dimensions"`
	Metric     vector.DistanceMetric `json:"metric"`
}

func collectionChange(ctx context.Context, client *common.Client, method, path string, body any) error {
	var response struct {
		Acknowledged *bool `json:"acknowledged"`
	}
	if err := client.Do(ctx, method, path, body, &response); err != nil {
		return err
	}
	if response.Acknowledged == nil {
		return status.Error(codes.Internal, "missing OpenSearch collection change acknowledgement")
	}
	if !*response.Acknowledged {
		return status.Error(codes.DeadlineExceeded, "OpenSearch collection change was not acknowledged; its outcome may be unknown")
	}
	return nil
}

func metricSpace(metric vector.DistanceMetric) (string, error) {
	switch metric {
	case vector.DistanceMetricUnspecified, vector.DistanceMetricCosine:
		return "cosinesimil", nil
	case vector.DistanceMetricDotProduct:
		return "innerproduct", nil
	case vector.DistanceMetricEuclidean:
		return "l2", nil
	default:
		return "", status.Error(codes.InvalidArgument, "unsupported distance metric")
	}
}

func (o *OpenSearch) CreateCollection(ctx context.Context, req *vector.CreateCollectionRequest) error {
	if req == nil || req.Dimensions == 0 {
		return status.Error(codes.InvalidArgument, "collection and positive dimensions are required")
	}
	path, err := common.IndexPath(req.Collection)
	if err != nil {
		return err
	}
	if _, err = metricSpace(req.Metric); err != nil {
		return err
	}
	client, err := o.ready()
	if err != nil {
		return err
	}
	metric := req.Metric
	if metric == vector.DistanceMetricUnspecified {
		metric = vector.DistanceMetricCosine
	}
	// Exact scoring does not require an ANN index. The metric is chosen at
	// query time, allowing overrides without rebuilding the collection.
	body := map[string]any{
		"mappings": map[string]any{
			"_meta":          collectionInfo{Kind: "vector", Dimensions: req.Dimensions, Metric: metric},
			"date_detection": false,
			"dynamic_templates": []any{map[string]any{
				"metadata_strings": map[string]any{
					"path_match": "metadata.*", "match_mapping_type": "string",
					"mapping": map[string]any{"type": "text", "fields": map[string]any{"keyword": map[string]any{"type": "keyword"}}},
				},
			}},
			"properties": map[string]any{
				"values":   map[string]any{"type": "knn_vector", "dimension": req.Dimensions},
				"payload":  map[string]any{"type": "binary"},
				"metadata": map[string]any{"type": "object", "dynamic": true},
			},
		},
	}
	return collectionChange(ctx, client, http.MethodPut, path, body)
}

func getCollectionInfo(ctx context.Context, client *common.Client, collection string) (collectionInfo, error) {
	path, err := common.IndexPath(collection)
	if err != nil {
		return collectionInfo{}, err
	}
	var response map[string]struct {
		Mappings struct {
			Meta       collectionInfo `json:"_meta"`
			Properties struct {
				Values struct {
					Type      string `json:"type"`
					Dimension uint32 `json:"dimension"`
				} `json:"values"`
			} `json:"properties"`
		} `json:"mappings"`
	}
	if err = client.Do(ctx, http.MethodGet, path+"/_mapping", nil, &response); err != nil {
		return collectionInfo{}, err
	}
	mapping, ok := response[collection]
	info := mapping.Mappings.Meta
	if !ok || info.Kind != "vector" || info.Dimensions == 0 ||
		info.Metric == vector.DistanceMetricUnspecified || mapping.Mappings.Properties.Values.Type != "knn_vector" ||
		mapping.Mappings.Properties.Values.Dimension != info.Dimensions {
		return collectionInfo{}, status.Error(codes.FailedPrecondition, "index is not a valid Dapr vector collection")
	}
	if _, err = metricSpace(info.Metric); err != nil {
		return collectionInfo{}, status.Error(codes.FailedPrecondition, "collection has an unsupported metric")
	}
	return info, nil
}

func (o *OpenSearch) GetCollection(ctx context.Context, req *vector.GetCollectionRequest) (*vector.GetCollectionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "collection is required")
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	info, err := getCollectionInfo(ctx, client, req.Collection)
	if err != nil {
		return nil, err
	}
	path, _ := common.IndexPath(req.Collection)
	var count struct {
		Count uint64 `json:"count"`
	}
	if err = client.Do(ctx, http.MethodGet, path+"/_count", nil, &count); err != nil {
		return nil, err
	}
	return &vector.GetCollectionResponse{
		Collection: req.Collection, Dimensions: info.Dimensions, Metric: info.Metric, RecordCount: count.Count,
	}, nil
}

func (o *OpenSearch) ListCollections(ctx context.Context, _ *vector.ListCollectionsRequest) (*vector.ListCollectionsResponse, error) {
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	names, err := client.ListIndexes(ctx, "vector")
	if err != nil {
		return nil, err
	}
	return &vector.ListCollectionsResponse{Collections: names}, nil
}

func (o *OpenSearch) DeleteCollection(ctx context.Context, req *vector.DeleteCollectionRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "collection is required")
	}
	client, err := o.ready()
	if err != nil {
		return err
	}
	if _, err = getCollectionInfo(ctx, client, req.Collection); err != nil {
		return err
	}
	path, _ := common.IndexPath(req.Collection)
	return collectionChange(ctx, client, http.MethodDelete, path, nil)
}

type storedRecord struct {
	Values   []float32      `json:"values"`
	Payload  []byte         `json:"payload,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func validateValues(values []float32, dimensions uint32, metric vector.DistanceMetric) error {
	if uint64(len(values)) != uint64(dimensions) {
		return status.Error(codes.InvalidArgument, "vector dimensions do not match the collection")
	}
	nonzero := false
	for _, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return status.Error(codes.InvalidArgument, "vector values must be finite")
		}
		nonzero = nonzero || value != 0
	}
	if metric == vector.DistanceMetricCosine && !nonzero {
		return status.Error(codes.InvalidArgument, "cosine vectors must have nonzero magnitude")
	}
	return nil
}

func validateIDs(ids []string, unique bool) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			return status.Error(codes.InvalidArgument, "record IDs must not be empty")
		}
		if _, exists := seen[id]; unique && exists {
			return status.Error(codes.InvalidArgument, "record IDs must be unique within an upsert")
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (o *OpenSearch) Upsert(ctx context.Context, req *vector.UpsertRequest) (*vector.UpsertResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "upsert request is required")
	}
	ctx, cancel, err := common.WithWriteContext(ctx, req.Options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	info, err := getCollectionInfo(ctx, client, req.Collection)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(req.Records))
	docs := make([]common.Document, len(req.Records))
	for i, record := range req.Records {
		ids[i] = record.ID
		if err = validateValues(record.Values, info.Dimensions, info.Metric); err != nil {
			return nil, err
		}
		source, marshalErr := json.Marshal(storedRecord{Values: record.Values, Payload: record.Payload, Metadata: record.Metadata})
		if marshalErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid record metadata: %v", marshalErr)
		}
		docs[i] = common.Document{ID: record.ID, Source: source}
	}
	if err = validateIDs(ids, true); err != nil {
		return nil, err
	}
	failed, err := client.Bulk(ctx, req.Collection, docs, nil)
	if err != nil {
		return nil, err
	}
	return &vector.UpsertResponse{Ack: search.IndexAckCompleted, FailedItems: failed}, nil
}

func decodeRecord(doc common.Document, includeValues bool) (vector.Record, error) {
	source := bytes.TrimSpace(doc.Source)
	if len(source) == 0 || source[0] != '{' {
		return vector.Record{}, status.Error(codes.Internal, "missing or invalid vector source")
	}
	var stored storedRecord
	if err := json.Unmarshal(doc.Source, &stored); err != nil {
		return vector.Record{}, status.Errorf(codes.Internal, "invalid vector source: %v", err)
	}
	record := vector.Record{ID: doc.ID, Payload: stored.Payload, Metadata: stored.Metadata}
	if includeValues {
		record.Values = stored.Values
	}
	return record, nil
}

func (o *OpenSearch) Get(ctx context.Context, req *vector.GetRequest) (*vector.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get request is required")
	}
	if err := validateIDs(req.IDs, false); err != nil {
		return nil, err
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	if _, err = getCollectionInfo(ctx, client, req.Collection); err != nil {
		return nil, err
	}
	docs, err := client.GetDocuments(ctx, req.Collection, req.IDs)
	if err != nil {
		return nil, err
	}
	out := &vector.GetResponse{Records: make([]vector.Record, 0, len(docs))}
	for _, doc := range docs {
		record, decodeErr := decodeRecord(doc, req.IncludeValues)
		if decodeErr != nil {
			return nil, decodeErr
		}
		out.Records = append(out.Records, record)
	}
	return out, nil
}

func (o *OpenSearch) Delete(ctx context.Context, req *vector.DeleteRequest) (*vector.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "delete request is required")
	}
	if err := validateIDs(req.IDs, false); err != nil {
		return nil, err
	}
	ctx, cancel, err := common.WithWriteContext(ctx, req.Options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	if _, err = getCollectionInfo(ctx, client, req.Collection); err != nil {
		return nil, err
	}
	failed, err := client.Bulk(ctx, req.Collection, nil, req.IDs)
	if err != nil {
		return nil, err
	}
	if len(failed) != 0 {
		return nil, failed[0].Error.Err()
	}
	return &vector.DeleteResponse{Ack: search.IndexAckCompleted}, nil
}

func (o *OpenSearch) Query(ctx context.Context, req *vector.QueryRequest) (*vector.QueryResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is required")
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	info, err := getCollectionInfo(ctx, client, req.Collection)
	if err != nil {
		return nil, err
	}
	return query(ctx, client, info, req)
}

func query(ctx context.Context, client *common.Client, info collectionInfo, req *vector.QueryRequest) (*vector.QueryResponse, error) {
	if (req.Vector == nil) == (req.ByID == "") {
		return nil, status.Error(codes.InvalidArgument, "exactly one of vector or by ID is required")
	}
	metric := req.Metric
	if metric == vector.DistanceMetricUnspecified {
		metric = info.Metric
	}
	space, err := metricSpace(metric)
	if err != nil {
		return nil, err
	}
	if req.ScoreThreshold != nil && (math.IsNaN(*req.ScoreThreshold) || math.IsInf(*req.ScoreThreshold, 0)) {
		return nil, status.Error(codes.InvalidArgument, "score threshold must be finite")
	}
	var values []float32
	if req.Vector != nil {
		values = req.Vector.Values
	} else {
		docs, getErr := client.GetDocuments(ctx, req.Collection, []string{req.ByID})
		if getErr != nil {
			return nil, getErr
		}
		if len(docs) == 0 {
			return nil, status.Error(codes.NotFound, "query vector was not found")
		}
		record, decodeErr := decodeRecord(docs[0], true)
		if decodeErr != nil {
			return nil, decodeErr
		}
		values = record.Values
	}
	if err = validateValues(values, info.Dimensions, metric); err != nil {
		return nil, err
	}
	filter, err := common.TranslateFilter(req.Filter, "metadata.")
	if err != nil {
		return nil, err
	}
	filters := []any{map[string]any{"exists": map[string]any{"field": "values"}}}
	if len(filter) != 0 {
		filters = append(filters, filter)
	}
	boolQuery := map[string]any{"filter": filters}
	if req.ByID != "" {
		boolQuery["must_not"] = []any{map[string]any{"ids": map[string]any{"values": []string{req.ByID}}}}
	}
	topK := req.TopK
	if topK == 0 {
		topK = 10
	}
	fields := []string{"metadata"}
	if req.IncludeValues {
		fields = append(fields, "values")
	}
	if req.IncludePayload {
		fields = append(fields, "payload")
	}
	script := map[string]any{
		"lang": "knn", "source": "knn_score",
		"params": map[string]any{"field": "values", "query_value": values, "space_type": space},
	}
	if metric == vector.DistanceMetricCosine {
		// knn_score changed its cosine normalization for indexes created in
		// OpenSearch 2.19+. The native Painless extension is version-independent.
		script = map[string]any{
			"lang": "painless", "source": "1.0 + cosineSimilarity(params.query_value, doc[params.field])",
			"params": map[string]any{"field": "values", "query_value": values},
		}
	}
	body := map[string]any{
		"size": topK, "_source": fields,
		"query": map[string]any{"script_score": map[string]any{
			"query":  map[string]any{"bool": boolQuery},
			"script": script,
		}},
	}
	var response struct {
		TimedOut bool `json:"timed_out"`
		Shards   struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
		Hits *struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Score  *float64        `json:"_score"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	path, _ := common.IndexPath(req.Collection)
	if err = client.Do(ctx, http.MethodPost, path+"/_search", body, &response); err != nil {
		return nil, err
	}
	if response.TimedOut {
		return nil, status.Error(codes.DeadlineExceeded, "OpenSearch vector search timed out")
	}
	if response.Shards.Failed != 0 {
		return nil, status.Error(codes.Internal, "OpenSearch vector search had failed shards")
	}
	if response.Hits == nil {
		return nil, status.Error(codes.Internal, "missing OpenSearch vector search hits")
	}
	out := &vector.QueryResponse{Metric: metric, Matches: make([]vector.Match, 0, len(response.Hits.Hits))}
	for _, hit := range response.Hits.Hits {
		if hit.ID == "" || hit.Score == nil {
			return nil, status.Error(codes.Internal, "missing vector match ID or score")
		}
		score, scoreErr := metricScore(*hit.Score, metric)
		if scoreErr != nil {
			return nil, scoreErr
		}
		// The server computes exact, prefiltered top-k in monotone score order.
		// Applying the inclusive cutoff here cannot hide a qualifying match.
		if req.ScoreThreshold != nil &&
			((metric.HigherIsBetter() && score < *req.ScoreThreshold) ||
				(!metric.HigherIsBetter() && score > *req.ScoreThreshold)) {
			continue
		}
		record, decodeErr := decodeRecord(common.Document{ID: hit.ID, Source: hit.Source}, req.IncludeValues)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if !req.IncludePayload {
			record.Payload = nil
		}
		out.Matches = append(out.Matches, vector.Match{Record: record, Score: score})
	}
	return out, nil
}

func metricScore(score float64, metric vector.DistanceMetric) (float64, error) {
	if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 ||
		(metric != vector.DistanceMetricCosine && score == 0) {
		return 0, status.Error(codes.Internal, "invalid OpenSearch vector score")
	}
	switch metric {
	case vector.DistanceMetricCosine:
		return score - 1, nil
	case vector.DistanceMetricDotProduct:
		if score >= 1 {
			return score - 1, nil
		}
		return 1 - 1/score, nil
	case vector.DistanceMetricEuclidean:
		if score > 1 {
			return 0, status.Error(codes.Internal, "invalid OpenSearch Euclidean score")
		}
		return math.Sqrt(1/score - 1), nil
	default:
		return 0, status.Error(codes.Internal, "invalid collection metric")
	}
}

func (o *OpenSearch) BatchQuery(ctx context.Context, req *vector.BatchQueryRequest) (*vector.BatchQueryResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "batch query request is required")
	}
	client, err := o.ready()
	if err != nil {
		return nil, err
	}
	info, err := getCollectionInfo(ctx, client, req.Collection)
	if err != nil {
		return nil, err
	}
	out := &vector.BatchQueryResponse{Results: make([]vector.BatchQueryResult, len(req.Queries))}
	for i := range req.Queries {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		request := req.Queries[i]
		request.Collection = req.Collection
		if request.Metadata == nil {
			request.Metadata = req.Metadata
		}
		response, queryErr := query(ctx, client, info, &request)
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		out.Results[i] = vector.BatchQueryResult{Response: response, Error: queryErr}
	}
	return out, nil
}
