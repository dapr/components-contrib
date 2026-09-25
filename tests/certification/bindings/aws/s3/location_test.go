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

package awss3binding_test

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func s3ExpectedLocation(endpoint, bucket, key string, forcePathStyle bool) (string, error) {
	if endpoint == "" {
		if forcePathStyle {
			return fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucket, key), nil
		}
		return fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucket, key), nil
	}

	location, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid S3 test endpoint: %w", err)
	}
	if (location.Scheme != "http" && location.Scheme != "https") || location.Hostname() == "" ||
		location.User != nil || location.Opaque != "" || location.RawQuery != "" ||
		location.ForceQuery || strings.Contains(endpoint, "#") {
		return "", fmt.Errorf("S3 test endpoint must be an HTTP(S) URL with a host and without credentials, query, or fragment")
	}
	if port := location.Port(); port != "" {
		number, parseErr := strconv.Atoi(port)
		if parseErr != nil || number < 1 || number > 65535 {
			return "", fmt.Errorf("S3 test endpoint port must be between 1 and 65535")
		}
	} else if strings.HasSuffix(location.Host, ":") {
		return "", fmt.Errorf("S3 test endpoint port must not be empty")
	}

	// Match the binding's small-upload location, not the SDK request URL.
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(endpoint, "/"), bucket, key), nil
}

func TestS3ExpectedLocation(t *testing.T) {
	for _, test := range []struct {
		name           string
		endpoint       string
		bucket         string
		key            string
		forcePathStyle bool
		expected       string
	}{
		{
			name: "DefaultVirtualHostedAWS", bucket: "test-bucket", key: "filename.txt",
			expected: "https://test-bucket.s3.amazonaws.com/filename.txt",
		},
		{
			name: "DefaultPathStyleAWS", bucket: "test-bucket", key: "filename.txt", forcePathStyle: true,
			expected: "https://s3.amazonaws.com/test-bucket/filename.txt",
		},
		{
			name: "IPv4PathStyleFalse", endpoint: "http://127.0.0.1:14671", bucket: "test-bucket", key: "filename.txt",
			expected: "http://127.0.0.1:14671/test-bucket/filename.txt",
		},
		{
			name: "IPv4PathStyleTrue", endpoint: "http://127.0.0.1:14671", bucket: "test-bucket", key: "filename.txt", forcePathStyle: true,
			expected: "http://127.0.0.1:14671/test-bucket/filename.txt",
		},
		{
			name: "IPv6PathStyleFalse", endpoint: "http://[::1]:4566", bucket: "test-bucket", key: "filename.txt",
			expected: "http://[::1]:4566/test-bucket/filename.txt",
		},
		{
			name: "IPv6PathStyleTrue", endpoint: "http://[::1]:4566", bucket: "test-bucket", key: "filename.txt", forcePathStyle: true,
			expected: "http://[::1]:4566/test-bucket/filename.txt",
		},
		{
			name: "HTTPSEndpoint", endpoint: "https://127.0.0.1:4566", bucket: "test-bucket", key: "filename.txt",
			expected: "https://127.0.0.1:4566/test-bucket/filename.txt",
		},
		{
			name: "TrailingSlash", endpoint: "http://127.0.0.1:4566/", bucket: "test-bucket", key: "filename.txt",
			expected: "http://127.0.0.1:4566/test-bucket/filename.txt",
		},
		{
			name: "EndpointPrefixAndObjectPath", endpoint: "http://127.0.0.1:4566/storage/", bucket: "test-bucket", key: "folder/filename.txt",
			expected: "http://127.0.0.1:4566/storage/test-bucket/folder/filename.txt",
		},
		{
			name: "DNSPathStyle", endpoint: "http://s3.local:4566", bucket: "test-bucket", key: "filename.txt", forcePathStyle: true,
			expected: "http://s3.local:4566/test-bucket/filename.txt",
		},
		{
			name: "DNSWithoutForcedPathStyle", endpoint: "http://s3.local:4566", bucket: "test-bucket", key: "filename.txt",
			expected: "http://s3.local:4566/test-bucket/filename.txt",
		},
		{
			name: "LocalhostWithoutForcedPathStyle", endpoint: "http://localhost:4566", bucket: "test-bucket", key: "filename.txt",
			expected: "http://localhost:4566/test-bucket/filename.txt",
		},
		{
			name: "LocalhostWithForcedPathStyle", endpoint: "http://localhost:4566", bucket: "test-bucket", key: "filename.txt", forcePathStyle: true,
			expected: "http://localhost:4566/test-bucket/filename.txt",
		},
		{
			name: "DNSOverTLS", endpoint: "https://s3.local", bucket: "test-bucket", key: "filename.txt",
			expected: "https://s3.local/test-bucket/filename.txt",
		},
		{
			name: "DNSWithEncodedPrefix", endpoint: "http://s3.local:4566/storage%2Fprefix/", bucket: "test-bucket", key: "folder/filename.txt",
			expected: "http://s3.local:4566/storage%2Fprefix/test-bucket/folder/filename.txt",
		},
		{
			name: "DottedBucketOverTLS", endpoint: "https://s3.local", bucket: "test.bucket", key: "filename.txt",
			expected: "https://s3.local/test.bucket/filename.txt",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, err := s3ExpectedLocation(test.endpoint, test.bucket, test.key, test.forcePathStyle)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}

	t.Run("InvalidEndpoint", func(t *testing.T) {
		for _, endpoint := range []string{
			" ",
			"127.0.0.1:4566",
			"ftp://127.0.0.1:4566",
			"http:///missing-host",
			"http://:4566",
			"http://127.0.0.1:",
			"http://127.0.0.1:0",
			"http://127.0.0.1:65536",
			"http://127.0.0.1:invalid",
			"http://[::1",
			"http://test:test@127.0.0.1:4566",
			"http://127.0.0.1:4566?query=value",
			"http://127.0.0.1:4566?",
			"http://127.0.0.1:4566#fragment",
			"http://127.0.0.1:4566#",
		} {
			actual, err := s3ExpectedLocation(endpoint, "test-bucket", "filename.txt", false)
			require.Error(t, err, "endpoint %q must not fall back to AWS", endpoint)
			require.Empty(t, actual)
		}
	})
}
