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

package testdata

type TestStruct struct {
	// APIKey is the API key for the Azure OpenAI API.
	APIKey string `json:"apiKey" mapstructure:"apiKey" authenticationProfile:"APIKey" binding:"output"`
	// Configuration to decode base64 file content before saving to bucket storage. (In case of saving a file with binary content).
	DecodeBase64 bool `json:"decodeBase64,string,omitempty" mapstructure:"decodeBase64"`
	//  Configuration to encode base64 file content before returning the content. (In case of opening a file with binary content).
	EncodeBase64 bool `json:"encodeBase64,string,omitempty" mapstructure:"encodeBase64"`
}
