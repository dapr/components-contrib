/*
Copyright 2024 The Dapr Authors
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

type s3Metadata struct {
	// AccessKey is the AWS access key to authenticate requests.
	AccessKey string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	// SecretKey is the AWS secret key associated with the access key.
	SecretKey string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	// SessionToken is the session token for temporary AWS credentials.
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken" mdignore:"true"`
	// Region specifies the AWS region for the bucket.
	Region string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	// AWS endpoint for the component to use, to connect to S3-compatible services or emulators. Do not use this when running against production AWS.
	Endpoint string `json:"endpoint,omitempty" mapstructure:"endpoint"`

	// The name of the S3 bucket to write to.
	Bucket string `json:"bucket" mapstructure:"bucket"`
	// Configuration to decode base64 file content before saving to bucket storage. (In case of saving a file with binary content).
	DecodeBase64 bool `json:"decodeBase64,string,omitempty" mapstructure:"decodeBase64"`
	//  Configuration to encode base64 file content before returning the content. (In case of opening a file with binary content).
	EncodeBase64 bool `json:"encodeBase64,string,omitempty" mapstructure:"encodeBase64"`
	// Currently Amazon S3 SDK supports virtual-hosted-style and path-style access. When false (the default), uses virtual-hosted-style format, i.e.: `https://<your bucket>.<endpoint>/<key>`. When true, uses path-style format, i.e.: `https://<endpoint>/<your bucket>/<key>`.
	ForcePathStyle bool `json:"forcePathStyle,string,omitempty" mapstructure:"forcePathStyle"`
	// Allows to connect to non-`https://` endpoints.
	DisableSSL bool `json:"disableSSL,string,omitempty" mapstructure:"disableSSL"`
	// When connecting to `https://` endpoints, accepts self-signed or invalid certificates.
	InsecureSSL bool `json:"insecureSSL,string,omitempty" mapstructure:"insecureSSL"`
	// Local path for the file to upload or download.
	FilePath string `json:"filePath" mapstructure:"filePath" mdignore:"true"`
	// Specifies the TTL for presigned URLs.
	PresignTTL string `json:"presignTTL" mapstructure:"presignTTL" mdignore:"true"`
	// Defines the storage class for uploaded objects.
	StorageClass string `json:"storageClass" mapstructure:"storageClass" mdignore:"true"`
}

// Set the default values here.
// This unifies the setup across all componets,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func Defaults() s3Metadata {
	return s3Metadata{}
}

// Note: we do not include any mdignored field.
func Examples() s3Metadata {
	return s3Metadata{
		Endpoint:       "http://localhost:4566",
		Bucket:         "bucket",
		DecodeBase64:   true,
		EncodeBase64:   true,
		ForcePathStyle: true,
		DisableSSL:     true,
		InsecureSSL:    true,
	}
}
