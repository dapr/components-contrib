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

package binarystore

const (
	// DefaultUploadPartSize is the buffer size, in bytes, that binary store
	// implementations use for each part of a streaming upload.
	//
	// Provider SDK defaults range from 1 MiB to 16 MiB, which makes upload
	// throughput, memory use, and (for Azure block blobs, where the 50,000
	// block limit turns the part size into a hard object-size ceiling) the
	// maximum supported file size differ from one component to the next.
	// Implementations therefore pass this value to their SDK explicitly
	// instead of relying on the SDK default.
	DefaultUploadPartSize int64 = 8 * 1024 * 1024

	// DefaultUploadConcurrency is the number of upload parts that binary
	// store implementations transfer in parallel. Combined with
	// DefaultUploadPartSize this bounds the buffered data per upload to
	// roughly 32 MiB while keeping several requests in flight.
	DefaultUploadConcurrency = 4
)
