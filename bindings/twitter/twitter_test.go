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
	"os"
	"testing"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	testTwitterConsumerKey    = "test-consumerKey"
	testTwitterConsumerSecret = "test-consumerSecret"
	testTwitterAccessToken    = "test-accessToken"
	testTwitterAccessSecret   = "test-accessSecret"
)

func getTestMetadata() bindings.Metadata {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"consumerKey":    testTwitterConsumerKey,
		"consumerSecret": testTwitterConsumerSecret,
		"accessToken":    testTwitterAccessToken,
		"accessSecret":   testTwitterAccessSecret,
	}

	return m
}

func getRuntimeMetadata() map[string]string {
	return map[string]string{
		"consumerKey":    os.Getenv("CONSUMER_KEY"),
		"consumerSecret": os.Getenv("CONSUMER_SECRET"),
		"accessToken":    os.Getenv("ACCESS_TOKEN"),
		"accessSecret":   os.Getenv("ACCESS_SECRET"),
	}
}

// go test -v -count=1 ./bindings/twitter/.
func TestInit(t *testing.T) {
	m := getTestMetadata()
	tw := NewTwitter(logger.NewLogger("test"))
	err := tw.Init(m)
	assert.Nilf(t, err, "error initializing valid metadata properties")
}

// TestReadError excutes the Read method and fails before the Twitter API call
// go test -v -count=1 -run TestReadError ./bindings/twitter/.
func TestReadError(t *testing.T) {
	tw := NewTwitter(logger.NewLogger("test"))
	m := getTestMetadata()
	err := tw.Init(m)
	assert.Nilf(t, err, "error initializing valid metadata properties")

	tw.Read(func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		t.Logf("result: %+v", res)
		assert.NotNilf(t, err, "no error on read with invalid credentials")

		return nil, nil
	})
}

// TestRead executes the Read method which calls Twiter API
// env RUN_LIVE_TW_TEST=true go test -v -count=1 -run TestReed ./bindings/twitter/.
func TestReed(t *testing.T) {
	if os.Getenv("RUN_LIVE_TW_TEST") != "true" {
		t.SkipNow() // skip this test until able to read credentials in test infra
	}
	m := bindings.Metadata{}
	m.Properties = getRuntimeMetadata()
	// add query
	m.Properties["query"] = "microsoft"
	tw := NewTwitter(logger.NewLogger("test"))
	tw.logger.SetOutputLevel(logger.DebugLevel)
	err := tw.Init(m)
	assert.Nilf(t, err, "error initializing read")

	counter := 0
	err = tw.Read(func(ctx context.Context, res *bindings.ReadResponse) ([]byte, error) {
		counter++
		t.Logf("tweet[%d]", counter)
		var tweet twitter.Tweet
		json.Unmarshal(res.Data, &tweet)
		assert.NotEmpty(t, tweet.IDStr, "tweet should have an ID")
		os.Exit(0)

		return nil, nil
	})
	assert.Nilf(t, err, "error on read")
}

// TestInvoke executes the Invoke method which calls Twiter API
// test tokens must be set
// env RUN_LIVE_TW_TEST=true go test -v -count=1 -run TestInvoke ./bindings/twitter/.
func TestInvoke(t *testing.T) {
	if os.Getenv("RUN_LIVE_TW_TEST") != "true" {
		t.SkipNow() // skip this test until able to read credentials in test infra
	}
	m := bindings.Metadata{}
	m.Properties = getRuntimeMetadata()
	tw := NewTwitter(logger.NewLogger("test"))
	tw.logger.SetOutputLevel(logger.DebugLevel)
	err := tw.Init(m)
	assert.Nilf(t, err, "error initializing Invoke")

	req := &bindings.InvokeRequest{
		Metadata: map[string]string{
			"query": "microsoft",
		},
	}

	resp, err := tw.Invoke(context.TODO(), req)
	assert.Nilf(t, err, "error on invoke")
	assert.NotNil(t, resp)
}
