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

package obs

import (
	"context"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// HuaweiOBSAPI holds only the necessary API functions from the OBS SDK
// The interface can also provide a way to implement stubs for the purpose of unit testing.
type HuaweiOBSAPI interface {
	PutObject(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error)
	PutFile(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error)
	GetObject(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error)
	DeleteObject(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error)
	ListObjects(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error)
	Close()
}

// HuaweiOBSService is a service layer which wraps the actual OBS SDK client to provide the API functions
// and it implements the HuaweiOBSAPI through wrapper functions.
type HuaweiOBSService struct {
	client *obs.ObsClient
}

func (s *HuaweiOBSService) PutObject(ctx context.Context, input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
	return s.client.PutObject(input, obs.WithRequestContext(ctx))
}

func (s *HuaweiOBSService) PutFile(ctx context.Context, input *obs.PutFileInput) (output *obs.PutObjectOutput, err error) {
	return s.client.PutFile(input, obs.WithRequestContext(ctx))
}

func (s *HuaweiOBSService) GetObject(ctx context.Context, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
	return s.client.GetObject(input, obs.WithRequestContext(ctx))
}

func (s *HuaweiOBSService) DeleteObject(ctx context.Context, input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
	return s.client.DeleteObject(input, obs.WithRequestContext(ctx))
}

func (s *HuaweiOBSService) ListObjects(ctx context.Context, input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
	return s.client.ListObjects(input, obs.WithRequestContext(ctx))
}

func (s *HuaweiOBSService) Close() {
	s.client.Close()
}
