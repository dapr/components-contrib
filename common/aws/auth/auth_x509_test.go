package auth

import (
	"github.com/dapr/components-contrib/common/aws/options"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthTypeIsx509(t *testing.T) {
	tests := []struct {
		options  options.Options
		expected bool
	}{
		{options: options.Options{
			Logger: nil,
			Properties: map[string]string{
				"trustProfileArn": "arn:aws:iam::123456789012:role/TrustProfile",
				"trustAnchorArn":  "arn:aws:iam::123456789012:role/TrustAnchor",
				"assumeRoleArn":   "arn:aws:iam::123456789012:role/AssumeRole",
			},
			Region:        "",
			AccessKey:     "",
			SecretKey:     "",
			SessionName:   "",
			AssumeRoleARN: "",
			SessionToken:  "",
			Endpoint:      "",
		}, expected: true},
		{options: options.Options{
			Logger:        nil,
			Properties:    nil,
			Region:        "",
			AccessKey:     "",
			SecretKey:     "",
			SessionName:   "",
			AssumeRoleARN: "",
			SessionToken:  "",
			Endpoint:      "",
		}, expected: false},
		{options: options.Options{
			Logger: nil,
			Properties: map[string]string{
				"trustProfileArn": "arn:aws:iam::123456789012:role/TrustProfile",
			},
			Region:        "",
			AccessKey:     "",
			SecretKey:     "",
			SessionName:   "",
			AssumeRoleARN: "",
			SessionToken:  "",
			Endpoint:      "",
		}, expected: false},
		{options: options.Options{
			Logger: nil,
			Properties: map[string]string{
				"trustProfileArn": "arn:aws:iam::123456789012:role/TrustProfile",
				"trustAnchorArn":  "arn:aws:iam::123456789012:role/TrustAnchor",
			},
			Region:        "",
			AccessKey:     "",
			SecretKey:     "",
			SessionName:   "",
			AssumeRoleARN: "",
			SessionToken:  "",
			Endpoint:      "",
		}, expected: false},
		{options: options.Options{
			Logger: nil,
			Properties: map[string]string{
				"trustProfileArn": "arn:aws:iam::123456789012:role/TrustProfile",
				"assumeRoleArn":   "arn:aws:iam::123456789012:role/AssumeRole",
			},
			Region:        "",
			AccessKey:     "",
			SecretKey:     "",
			SessionName:   "",
			AssumeRoleARN: "",
			SessionToken:  "",
			Endpoint:      "",
		}, expected: false},
		{options: options.Options{
			Logger: nil,
			Properties: map[string]string{
				"trustanchorArn": "arn:aws:iam::123456789012:role/TrustAnchor",
				"assumeRoleArn":  "arn:aws:iam::123456789012:role/AssumeRole",
			},
			Region:        "",
			AccessKey:     "",
			SecretKey:     "",
			SessionName:   "",
			AssumeRoleARN: "",
			SessionToken:  "",
			Endpoint:      "",
		}, expected: false},
	}

	for _, test := range tests {
		result := authTypeIsx509(test.options)
		assert.Equal(t, test.expected, result, "authTypeIsX509(%v) = %v; want %v", test.options.Properties, result,
			test.expected)
	}
}
