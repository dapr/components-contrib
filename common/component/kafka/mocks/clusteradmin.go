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

package mocks

import "github.com/IBM/sarama"

// FakeClusterAdmin implements sarama.ClusterAdmin for unit testing.
// Only CreateTopic and Close carry injectable behaviour; every other method
// returns a sensible zero-value so the mock satisfies the full interface.
type FakeClusterAdmin struct {
	createTopicFn func(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	closeFn       func() error
}

func NewClusterAdmin() *FakeClusterAdmin {
	return &FakeClusterAdmin{
		createTopicFn: func(string, *sarama.TopicDetail, bool) error {
			return nil
		},
		closeFn: func() error {
			return nil
		},
	}
}

func (f *FakeClusterAdmin) WithCreateTopicFn(fn func(string, *sarama.TopicDetail, bool) error) *FakeClusterAdmin {
	f.createTopicFn = fn
	return f
}

func (f *FakeClusterAdmin) WithCloseFn(fn func() error) *FakeClusterAdmin {
	f.closeFn = fn
	return f
}

// --- sarama.ClusterAdmin interface ---

func (f *FakeClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	return f.createTopicFn(topic, detail, validateOnly)
}

func (f *FakeClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DescribeTopics([]string) ([]*sarama.TopicMetadata, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DeleteTopic(string) error { return nil }

func (f *FakeClusterAdmin) CreatePartitions(string, int32, [][]int32, bool) error { return nil }

func (f *FakeClusterAdmin) AlterPartitionReassignments(string, [][]int32) error { return nil }

func (f *FakeClusterAdmin) ListPartitionReassignments(string, []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DeleteRecords(string, map[int32]int64) error { return nil }

func (f *FakeClusterAdmin) DescribeConfig(sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) AlterConfig(sarama.ConfigResourceType, string, map[string]*string, bool) error {
	return nil
}

func (f *FakeClusterAdmin) IncrementalAlterConfig(sarama.ConfigResourceType, string, map[string]sarama.IncrementalAlterConfigsEntry, bool) error {
	return nil
}

func (f *FakeClusterAdmin) CreateACL(sarama.Resource, sarama.Acl) error { return nil }

func (f *FakeClusterAdmin) CreateACLs([]*sarama.ResourceAcls) error { return nil }

func (f *FakeClusterAdmin) ListAcls(sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DeleteACL(sarama.AclFilter, bool) ([]sarama.MatchingAcl, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) ListConsumerGroups() (map[string]string, error) { return nil, nil }

func (f *FakeClusterAdmin) DescribeConsumerGroups([]string) ([]*sarama.GroupDescription, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) ListConsumerGroupOffsets(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DeleteConsumerGroupOffset(string, string, int32) error { return nil }

func (f *FakeClusterAdmin) DeleteConsumerGroup(string) error { return nil }

func (f *FakeClusterAdmin) DescribeCluster() ([]*sarama.Broker, int32, error) { return nil, 0, nil }

func (f *FakeClusterAdmin) DescribeLogDirs([]int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DescribeUserScramCredentials([]string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DeleteUserScramCredentials([]sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) UpsertUserScramCredentials([]sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) DescribeClientQuotas([]sarama.QuotaFilterComponent, bool) ([]sarama.DescribeClientQuotasEntry, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) AlterClientQuotas([]sarama.QuotaEntityComponent, sarama.ClientQuotasOp, bool) error {
	return nil
}

func (f *FakeClusterAdmin) ElectLeaders(sarama.ElectionType, map[string][]int32) (map[string]map[int32]*sarama.PartitionResult, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) Controller() (*sarama.Broker, error) { return nil, nil }

func (f *FakeClusterAdmin) Coordinator(string) (*sarama.Broker, error) { return nil, nil }

func (f *FakeClusterAdmin) RemoveMemberFromConsumerGroup(string, []string) (*sarama.LeaveGroupResponse, error) {
	return nil, nil
}

func (f *FakeClusterAdmin) Close() error {
	return f.closeFn()
}
