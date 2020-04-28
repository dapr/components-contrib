// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package twitter

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

const (
	testTwitterConsumerKey    = "test-consumerKey"
	testTwitterConsumerSecret = "test-consumerSecret"
	testTwitterAccessToken    = "test-accessToken"
	testTwitterAccessSecret   = "test-accessSecret"
	testTwitterQuery          = "test-query"
)

func getTestMetadata() bindings.Metadata {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"consumerKey":    testTwitterConsumerKey,
		"consumerSecret": testTwitterConsumerSecret,
		"accessToken":    testTwitterAccessToken,
		"accessSecret":   testTwitterAccessSecret,
		"query":          testTwitterQuery,
	}
	return m
}

func TestParseMetadata(t *testing.T) {

	m := getTestMetadata()
	i := twitterInput{logger: logger.NewLogger("test")}
	err := i.parseMetadata(m)
	assert.Nilf(t, err, "error parsing valid metadata properties")
	assert.Equal(t, testTwitterConsumerKey, i.consumerKey, "consumerKey should be the same")
	assert.Equal(t, testTwitterConsumerSecret, i.consumerSecret, "consumerSecret should be the same")
	assert.Equal(t, testTwitterAccessToken, i.accessToken, "accessToken should be the same")
	assert.Equal(t, testTwitterAccessSecret, i.accessSecret, "accessSecret should be the same")

	m.Properties["consumerKey"] = ""
	err = i.parseMetadata(m)
	assert.NotNilf(t, err, "no error parsing invalid metadata properties")

	m.Properties["consumerKey"] = testTwitterConsumerKey
	m.Properties["query"] = ""
	err = i.parseMetadata(m)
	assert.NotNilf(t, err, "no error parsing invalid metadata properties")

}

func TestInit(t *testing.T) {
	m := getTestMetadata()
	tw := NewTwitter(logger.NewLogger("test"))
	err := tw.Init(m)
	assert.Nilf(t, err, "error initializing valid metadata properties")
}

func TestReadError(t *testing.T) {
	m := getTestMetadata()
	tw := NewTwitter(logger.NewLogger("test"))
	err := tw.Init(m)
	assert.Nilf(t, err, "error initializing valid metadata properties")

	tw.Read(func(res *bindings.ReadResponse) error {
		t.Logf("result: %+v", res)
		assert.NotNilf(t, err, "no error on read with invalid credentials")
		return nil
	})

}
