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

package rabbitmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func getFakeClientKey() string {
	return "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJWaOygL4rEXiW\n4f2Fm27bptAAu+66gKmdVKM3IJmuKXsz8RYtLwyWevjv1wZzrcRSH7KtffWbARIS\n5Hb8YS6zNaFEZP/gd7ZR0bMpyQKVaxDJJzdeTql91VlRYEY46BVdjg88lpnzr73q\n9TzM6HRRRoxJbmJwXy7gySapGoeKo/LCQCpRgu4X2fR/iPxS20XzMiqQKpThDf8D\nSunX5+RvzUDV7Ym04/QdeaKxeUoPxg7tRy/bq65SOzblDLpuLhcuf5xmo8MF9dXg\ngtZ3ZJykJOeecjfGLHZlVW5sxF5I+fW3jbKqE6vonx16sj99fA5Vh7I6OLQfdhKq\nUpnpqimvAgMBAAECggEAFNQoinEl+veSNW2IV9dl5uQBNWo7fmCL60ITpkLBkxIo\ndA2XATKSR0jOUqU4PiQ2IFa2GZtjmlbIg1rN8jqtZY2BMr2N+K75lcovEf4Aves2\n79AWOrPBdcpptHDUotNjTmvBKL/diideap1W3k2Xpsa5AWmhVkWKaGd2j9y2M8Gl\n5x7HDS+MSBEeh/1U8iuWmEMDsstA6ZIwIFa3oC7Ns66kiIdYYLrZEaQ/OIpqkElP\n8ulF2rloVF1k/UxD8fmmqTZ7v8B8RbTnw6Hsjg2CvXuzJqm+KL1ZidNGnId0hJ0N\n8UI6WpxFBfZXOMdocWZp6KyTzJe6sG83VQowI5iIjQKBgQDTpxteQwxOIFM29wMX\nAzu9+cYEUg5O0ro1l23NfW1iVfdL5NFU7ql4mHjlM+huqPPuavAtdnPcLYfqKYSi\nq5o5WGaSLlnBXE0Jfue5IJDQ3+zKwUb3bMpmkY+H5a8ssrRxp4Svsm9OF4pmvB/p\nwj7vJHLB0gPRflqEVvPrnVjutQKBgQDzieiUs04IfBb241MD716Ns2QIRIgXlDRr\nPqHPAQaRVoX9bnsfJ0Py/tOVVvzn6WcVuE+KowXSyzyCa9i0AW1OukgNvdwCEiMW\nj6Xx5Wj1lj9gbGGTvqhOM54CLQ7scbdc5IwOeL3s8M4hvENn/vy6VDzadih9ModH\nGVShNAfRUwKBgQCU5MIEFfbaHzNrP2oVDllA1C3RbEhUezoz6Ut9e/cvbnqCEX6R\n2TOvetPRqiqhbonr957u3J21DvLGcF62cTIVlXLS83iA5oXrYDNzsM0vo9fY6w8Z\nkJbygQQP/iy1rSHzsn1NfKGAtl7O1xk8QUI0yddRMoUtI05aOzFtV0vw4QKBgGSt\nzXWzzn2ZdxDnl0kYKtmfoKn+UtiUjzsKsG/YN7Pbtha4FrQhUmjhSe0CAhmJlvEW\nueNRU7X6CPDNzs9Ti7GxNtpfc9VzVAgeqHag5Uwpqded1pTyi7eQhTDXGcVLmYcQ\ngIn3BmUug7jUkHUsp6QL8QBLOp/PRLdy1Fa0IMs7AoGBAKS6oa/nN+Q6g1dv5szZ\n2Lq5sXOuhBio2KOm2LKj+m1nCQfHuvdCIpoeyaCqSjfEqW2dsQDcbM4ZRnlICV7c\n0ktWwIWlN4mtiAf+cUgj6ormEXIxcDt6/w8k/e/ME8vDb7URCs/4z1rcFbwM8uZ0\nPY3PxxuKXsQvfO05TX4rc/7p\n-----END PRIVATE KEY-----\n"
}

func getFakeClientCert() string {
	return "-----BEGIN CERTIFICATE-----\nMIIDKDCCAhCgAwIBAgIBAjANBgkqhkiG9w0BAQsFADATMREwDwYDVQQDDAhNeVRl\nc3RDQTAeFw0yMzAyMDcwODU5MDFaFw0yNDAyMDcwODU5MDFaMCsxGDAWBgNVBAMM\nD2J1aWxka2l0c2FuZGJveDEPMA0GA1UECgwGY2xpZW50MIIBIjANBgkqhkiG9w0B\nAQEFAAOCAQ8AMIIBCgKCAQEAyVmjsoC+KxF4luH9hZtu26bQALvuuoCpnVSjNyCZ\nril7M/EWLS8Mlnr479cGc63EUh+yrX31mwESEuR2/GEuszWhRGT/4He2UdGzKckC\nlWsQySc3Xk6pfdVZUWBGOOgVXY4PPJaZ86+96vU8zOh0UUaMSW5icF8u4MkmqRqH\niqPywkAqUYLuF9n0f4j8UttF8zIqkCqU4Q3/A0rp1+fkb81A1e2JtOP0HXmisXlK\nD8YO7Ucv26uuUjs25Qy6bi4XLn+cZqPDBfXV4ILWd2ScpCTnnnI3xix2ZVVubMRe\nSPn1t42yqhOr6J8derI/fXwOVYeyOji0H3YSqlKZ6aoprwIDAQABo28wbTAJBgNV\nHRMEAjAAMAsGA1UdDwQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDAjAdBgNVHQ4E\nFgQUkYN3AXGx/Q7J08JYk5tGbvi5MkQwHwYDVR0jBBgwFoAUMYsP7MzQJRxhA29d\nJCS8ERM/kkkwDQYJKoZIhvcNAQELBQADggEBAJ80qe/b8GgCEGUsGGEN44L/bmQd\nUMM73FyAsxjOuUowZn4c0WqRAqCFWRaxH4dye5juL2ZHi/mnQ1PbE3IuPWHN+hDK\nD/NyjepMyMUKrs1DVpry3t8QxdyVB6Y19MNevLYQznpRNlhgUbxVSA7hyWiuZt64\ng3ueY9dQJagFuGo3Ez/gwS+m6HLJfAtDEaWpJpmhTjCPXp2jKctKxVITxWWj82la\nPD12HBivvsA1K69klzR0Yz2KF1rLOMNDxaY/4TkMkbj7ipWZTttvZ8rp76Tuuoyy\nGZPBtmLFBDzBWlJcSFv/ODeyB6nR89UeRo9lDwF3msFMT2zVllRqPdU2ytk=\n-----END CERTIFICATE-----\n"
}

func getFakeCaCert() string {
	return "-----BEGIN CERTIFICATE-----\nMIIC8DCCAdigAwIBAgIUXIbafzX6lYtuuGQhKrCpBPlBAWgwDQYJKoZIhvcNAQEL\nBQAwEzERMA8GA1UEAwwITXlUZXN0Q0EwHhcNMjMwMjA3MDg1OTAxWhcNMjQwMjA3\nMDg1OTAxWjATMREwDwYDVQQDDAhNeVRlc3RDQTCCASIwDQYJKoZIhvcNAQEBBQAD\nggEPADCCAQoCggEBAKDJ/Vmx4KH0WxMiiAyJdV04wF70gXDDGfr553VkGMVg+vId\nL95czDqEEcgvYLyAnmOYFarxVLmEKNgE7IjvO3fUk5skVWa9Cn5QAwAumy2WnBxv\nCn85Ozdqqw+GyctPBXa93fCNSuqx7HSvHkLJF9piqPSq7F+DpOcE/F+vLNg0A3an\nT32HpwgVGeyAqlbu/MyN+y18oFVwzKALENV9Q+puG5tyhTAT7/Ocp/D9OrwkaB99\nFyz2B4lO/T9zWDDvcSL0uVoW/tFaJEInU3y5DfRtj5+o/+movdfjew0CVLYhSzvA\nzX/ABuCo5SuMH6TOhGettZddIMz9z+YAWJdyYxUCAwEAAaM8MDowDAYDVR0TBAUw\nAwEB/zALBgNVHQ8EBAMCAQYwHQYDVR0OBBYEFDGLD+zM0CUcYQNvXSQkvBETP5JJ\nMA0GCSqGSIb3DQEBCwUAA4IBAQCNQpDIGJq9bLKPHE7xwZLu9hvXBziVKlH5OHCF\nluylLJsa0lJjHqTBXlUqyM0gRLtWHG1FD1skNfB22MTBW+UZz3fVW2hwyBacjCcB\n71/aKgB6VzWQOqy16UbCAX7RqHyiI7GXojfJ7KakmtXduzE89CMTq0d+23QHr75N\nC+MXYtRCIEmXbeJeDhi8ub9GwDt+hQ/9G6tLSFZ4h/dus7qkDCpEToVtcbbCcsES\nSw58rDjoM++IzbOuIP4fJq4EnVvAhgomxr0snV3Emz37/UyQVCi2jHclSJ1g6/pL\nUoJQtHOjA2oC/lQrIUqau+2quIml9QAzlC4vaXkqIVA/D6qN\n-----END CERTIFICATE-----\n"
}

func TestParseMetadata(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"
	oneSecondTTL := time.Second

	testCases := []struct {
		name                       string
		properties                 map[string]string
		expectedDeleteWhenUnused   bool
		expectedDurable            bool
		expectedExclusive          bool
		expectedTTL                *time.Duration
		expectedPrefetchCount      int
		expectedMaxPriority        *uint8
		expectedReconnectWaitCheck func(expect time.Duration) bool
		expectedClientCert         string
		expectedClientKey          string
		expectedCACert             string
		expectedSaslExternal       bool
	}{
		{
			name:                     "Delete / Durable",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "true", "durable": "true"},
			expectedDeleteWhenUnused: true,
			expectedDurable:          true,
			expectedReconnectWaitCheck: func(expect time.Duration) bool {
				return expect == defaultReconnectWait
			},
		},
		{
			name:                     "Not Delete / Not durable",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
		},
		{
			name:                     "With one second TTL",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", metadata.TTLMetadataKey: "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedTTL:              &oneSecondTTL,
		},
		{
			name:                     "Empty TTL",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", metadata.TTLMetadataKey: ""},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedTTL:              ptr.Of(time.Duration(0)),
		},
		{
			name:                     "With one prefetchCount",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "prefetchCount": "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedPrefetchCount:    1,
		},
		{
			name:                     "Exclusive Queue",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "exclusive": "true"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedExclusive:        true,
		},
		{
			name:                     "With maxPriority",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "maxPriority": "1"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedMaxPriority: func() *uint8 {
				v := uint8(1)

				return &v
			}(),
		},
		{
			name:                     "With maxPriority(> 255)",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "maxPriority": "256"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedMaxPriority: func() *uint8 {
				v := uint8(255)

				return &v
			}(),
		},
		{
			name:                     "With reconnectWait 10 second",
			properties:               map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "reconnectWaitInSeconds": "10"},
			expectedDeleteWhenUnused: false,
			expectedDurable:          false,
			expectedReconnectWaitCheck: func(expect time.Duration) bool {
				return expect == 10*time.Second
			},
		},
		{
			name:                 "With Certificates and external SASL",
			properties:           map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "saslExternal": "true", "ClientCert": getFakeClientCert(), "ClientKey": getFakeClientKey(), "CaCert": getFakeCaCert()},
			expectedSaslExternal: true,
			expectedCACert:       getFakeCaCert(),
			expectedClientCert:   getFakeClientCert(),
			expectedClientKey:    getFakeClientKey(),
		},
		{
			name:                 "With Certificates and not external SASL",
			properties:           map[string]string{"queueName": queueName, "host": host, "deleteWhenUnused": "false", "durable": "false", "saslExternal": "false", "ClientCert": getFakeClientCert(), "ClientKey": getFakeClientKey(), "CaCert": getFakeCaCert()},
			expectedSaslExternal: false,
			expectedCACert:       getFakeCaCert(),
			expectedClientCert:   getFakeClientCert(),
			expectedClientKey:    getFakeClientKey(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.NoError(t, err)
			assert.Equal(t, queueName, r.metadata.QueueName)
			assert.Equal(t, host, r.metadata.Host)
			assert.Equal(t, tt.expectedDeleteWhenUnused, r.metadata.DeleteWhenUnused)
			assert.Equal(t, tt.expectedDurable, r.metadata.Durable)
			assert.Equal(t, tt.expectedTTL, r.metadata.DefaultQueueTTL)
			assert.Equal(t, tt.expectedPrefetchCount, r.metadata.PrefetchCount)
			assert.Equal(t, tt.expectedExclusive, r.metadata.Exclusive)
			assert.Equal(t, tt.expectedMaxPriority, r.metadata.MaxPriority)
			assert.Equal(t, tt.expectedClientCert, r.metadata.ClientCert)
			assert.Equal(t, tt.expectedClientKey, r.metadata.ClientKey)
			assert.Equal(t, tt.expectedCACert, r.metadata.CaCert)
			assert.Equal(t, tt.expectedSaslExternal, r.metadata.ExternalSasl)
			if tt.expectedReconnectWaitCheck != nil {
				assert.True(t, tt.expectedReconnectWaitCheck(r.metadata.ReconnectWait))
			}
		})
	}
}

func TestParseMetadataWithInvalidTTL(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"

	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces TTL",
			properties: map[string]string{"queueName": queueName, "host": host, metadata.TTLMetadataKey: "  "},
		},
		{
			name:       "Negative ttl",
			properties: map[string]string{"queueName": queueName, "host": host, metadata.TTLMetadataKey: "-1"},
		},
		{
			name:       "Non-numeric ttl",
			properties: map[string]string{"queueName": queueName, "host": host, metadata.TTLMetadataKey: "abc"},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}

func TestParseMetadataWithInvalidMaxPriority(t *testing.T) {
	const queueName = "test-queue"
	const host = "test-host"

	testCases := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "Whitespaces maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "  "},
		},
		{
			name:       "Negative maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "-1"},
		},
		{
			name:       "Non-numeric maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "abc"},
		},
		{
			name:       "Negative maxPriority",
			properties: map[string]string{"queueName": queueName, "host": host, "maxPriority": "-1"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m := bindings.Metadata{}
			m.Properties = tt.properties
			r := RabbitMQ{logger: logger.NewLogger("test")}
			err := r.parseMetadata(m)
			assert.NotNil(t, err)
		})
	}
}
