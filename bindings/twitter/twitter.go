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

package twitter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// Binding represents Twitter input/output binding.
type Binding struct {
	client *twitter.Client
	query  string
	logger logger.Logger
}

// NewTwitter returns a new Twitter event input binding.
func NewTwitter(logger logger.Logger) bindings.InputOutputBinding {
	return &Binding{logger: logger}
}

// Init initializes the Twitter binding.
func (t *Binding) Init(metadata bindings.Metadata) error {
	ck, f := metadata.Properties["consumerKey"]
	if !f || ck == "" {
		return fmt.Errorf("consumerKey not set")
	}
	cs, f := metadata.Properties["consumerSecret"]
	if !f || cs == "" {
		return fmt.Errorf("consumerSecret not set")
	}
	at, f := metadata.Properties["accessToken"]
	if !f || at == "" {
		return fmt.Errorf("accessToken not set")
	}
	as, f := metadata.Properties["accessSecret"]
	if !f || as == "" {
		return fmt.Errorf("accessSecret not set")
	}

	// set query only in an input binding case
	q, f := metadata.Properties["query"]
	if f {
		t.query = q
	}

	config := oauth1.NewConfig(ck, cs)
	token := oauth1.NewToken(at, as)

	httpClient := config.Client(oauth1.NoContext, token)

	t.client = twitter.NewClient(httpClient)

	return nil
}

// Operations returns list of operations supported by twitter binding.
func (t *Binding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.GetOperation}
}

// Read triggers the Twitter search and events on each result tweet.
func (t *Binding) Read(ctx context.Context, handler bindings.Handler) error {
	if t.query == "" {
		return errors.New("metadata property 'query' is empty")
	}

	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		t.logger.Debugf("raw tweet: %+v", tweet)
		data, marshalErr := json.Marshal(tweet)
		if marshalErr != nil {
			t.logger.Errorf("error marshaling tweet: %+v", tweet)

			return
		}
		handler(ctx, &bindings.ReadResponse{
			Data: data,
			Metadata: map[string]string{
				"query": t.query,
			},
		})
	}

	demux.StreamLimit = func(limit *twitter.StreamLimit) {
		t.logger.Warnf("disconnect: %+v", limit)
	}

	demux.StreamDisconnect = func(disconnect *twitter.StreamDisconnect) {
		t.logger.Errorf("stream disconnect: %+v", disconnect)
	}

	filterParams := &twitter.StreamFilterParams{
		Track:         []string{t.query},
		StallWarnings: twitter.Bool(true),
	}

	t.logger.Debug("starting stream for query: %s", t.query)
	stream, err := t.client.Streams.Filter(filterParams)
	if err != nil {
		return errors.Wrapf(err, "error executing stream filter: %+v", filterParams)
	}

	t.logger.Debug("starting handler...")
	go demux.HandleChan(stream.Messages)

	go func() {
		<-ctx.Done()
		t.logger.Debug("stopping handler...")
		stream.Stop()
	}()

	return nil
}

// Invoke handles all operations.
func (t *Binding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	t.logger.Debugf("operation: %v", req.Operation)
	if req.Metadata == nil {
		return nil, fmt.Errorf("metadata not set")
	}
	// required
	q, f := req.Metadata["query"]
	if !f || q == "" {
		return nil, fmt.Errorf("query not set")
	}

	// optionals
	l, f := req.Metadata["lang"]
	if !f || l == "" {
		l = "en"
	}

	r, f := req.Metadata["result"]
	if !f || r == "" {
		// mixed : Include both popular and real time results in the response
		// recent : return only the most recent results in the response
		// popular : return only the most popular results in the response
		r = "recent"
	}

	var sinceID int64
	s, f := req.Metadata["since_id"]
	if f && s != "" {
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			sinceID = i
		}
	}

	sq := &twitter.SearchTweetParams{
		Count:           100, // max
		Lang:            l,
		SinceID:         sinceID,
		Query:           q,
		ResultType:      r,
		IncludeEntities: twitter.Bool(true),
	}

	t.logger.Debug("starting stream for: %+v", sq)
	search, _, err := t.client.Search.Tweets(sq)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing search filter: %+v", sq)
	}
	if search == nil || search.Statuses == nil {
		return nil, errors.Wrapf(err, "nil search result from: %+v", sq)
	}

	t.logger.Debugf("raw response: %+v", search.Statuses)
	data, marshalErr := json.Marshal(search.Statuses)
	if marshalErr != nil {
		t.logger.Errorf("error marshaling tweet: %v", marshalErr)

		return nil, errors.Wrapf(err, "error parsing response from: %+v", sq)
	}

	req.Metadata["max_tweet_id"] = search.Metadata.MaxIDStr
	req.Metadata["tweet_count"] = strconv.Itoa(search.Metadata.Count)
	req.Metadata["search_ts"] = time.Now().UTC().String()

	ir := &bindings.InvokeResponse{
		Data:     data,
		Metadata: req.Metadata,
	}

	return ir, nil
}
