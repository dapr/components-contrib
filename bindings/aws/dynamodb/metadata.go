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

package dynamodb

type dynamoDBMetadata struct {
	Region       string `json:"region" mapstructure:"region"`
	Endpoint     string `json:"endpoint" mapstructure:"endpoint"`
	AccessKey    string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey    string `json:"secretKey" mapstructure:"secretKey"`
	SessionToken string `json:"sessionToken" mapstructure:"sessionToken"`
	Table        string `json:"table" mapstructure:"table"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func Defaults() dynamoDBMetadata {
	return dynamoDBMetadata{}
}

// Note: we do not include any mdignored field.
func Examples() dynamoDBMetadata {
	return dynamoDBMetadata{
		Endpoint: "http://localhost:4566",
		Table:    "Contracts",
	}
}
