/*
Copyright 2025 The Dapr Authors
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

package s3

import (
	"fmt"
	"strings"

	kitmd "github.com/dapr/kit/metadata"
)

// s3Metadata contains the parsed metadata used to construct an S3 (or
// S3-compatible) client.
type s3Metadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	AccessKey    string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`

	Region         string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	Endpoint       string `json:"endpoint" mapstructure:"endpoint"`
	Bucket         string `json:"bucket" mapstructure:"bucket"`
	ForcePathStyle bool   `json:"forcePathStyle,string" mapstructure:"forcePathStyle"`
	DisableSSL     bool   `json:"disableSSL,string" mapstructure:"disableSSL"`
	InsecureSSL    bool   `json:"insecureSSL,string" mapstructure:"insecureSSL"`
}

func parseMetadata(meta map[string]string) (*s3Metadata, error) {
	m := s3Metadata{}
	decodeErr := kitmd.DecodeMetadata(meta, &m)
	if decodeErr != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", decodeErr)
	}

	if m.Bucket == "" {
		return nil, fmt.Errorf("missing or empty bucket field from metadata")
	}

	if m.DisableSSL && m.Endpoint != "" && !strings.HasPrefix(m.Endpoint, "http://") && !strings.HasPrefix(m.Endpoint, "https://") {
		m.Endpoint = "http://" + m.Endpoint
	}

	return &m, nil
}
