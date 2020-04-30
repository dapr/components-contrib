// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package twitter

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

type twitterInput struct {
	consumerKey    string
	consumerSecret string
	accessToken    string
	accessSecret   string
	query          string
	logger         logger.Logger
}

var _ = bindings.InputBinding(&twitterInput{})

// NewTwitter returns a new Twitter event input binding
func NewTwitter(logger logger.Logger) bindings.InputBinding {
	return &twitterInput{logger: logger}
}

// Init initializes the Twitter binding
func (t *twitterInput) Init(metadata bindings.Metadata) error {
	return t.parseMetadata(metadata)
}

func (t *twitterInput) parseMetadata(metadata bindings.Metadata) error {
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
	q, f := metadata.Properties["query"]
	if !f || q == "" {
		return fmt.Errorf("query not set")
	}

	t.consumerKey = ck
	t.consumerSecret = cs
	t.accessToken = at
	t.accessSecret = as
	t.query = q

	return nil
}

// Read triggers the Twitter search and events on each result tweet
func (t *twitterInput) Read(handler func(*bindings.ReadResponse) error) error {
	config := oauth1.NewConfig(t.consumerKey, t.consumerSecret)
	token := oauth1.NewToken(t.accessToken, t.accessSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	client := twitter.NewClient(httpClient)

	limit, _, err := client.RateLimits.Status(nil)
	if err != nil {
		return errors.Wrapf(err, "error checking rate status: %v", err)
	}
	t.logger.Debugf("rate limit: %+v", limit)

	demux := twitter.NewSwitchDemux()

	demux.Tweet = func(tweet *twitter.Tweet) {
		t.logger.Debugf("raw tweet: %+v", tweet)
		data, marshalErr := json.Marshal(tweet)
		if marshalErr != nil {
			t.logger.Errorf("error marshaling tweet: %+v", tweet)
			return
		}
		handler(&bindings.ReadResponse{
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
	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		return errors.Wrapf(err, "error executing stream filter: %+v", filterParams)
	}
	defer stream.Stop()

	t.logger.Debug("starting handler...")
	go demux.HandleChan(stream.Messages)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	done := false
	for !done {
		s := <-signalChan
		switch s {
		case syscall.SIGHUP:
			t.logger.Info("stopping, component hung up")
			done = true
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			t.logger.Info("stopping, component terminated")
			done = true
		}
	}
	return nil
}
