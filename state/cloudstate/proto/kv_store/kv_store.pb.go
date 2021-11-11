// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.18.0
// source: kv_store.proto

package kv_store

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type DeleteStateEnvelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key     string        `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Options *StateOptions `protobuf:"bytes,2,opt,name=options,proto3" json:"options,omitempty"`
	Etag    string        `protobuf:"bytes,3,opt,name=etag,proto3" json:"etag,omitempty"`
}

func (x *DeleteStateEnvelope) Reset() {
	*x = DeleteStateEnvelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteStateEnvelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteStateEnvelope) ProtoMessage() {}

func (x *DeleteStateEnvelope) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteStateEnvelope.ProtoReflect.Descriptor instead.
func (*DeleteStateEnvelope) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{0}
}

func (x *DeleteStateEnvelope) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *DeleteStateEnvelope) GetOptions() *StateOptions {
	if x != nil {
		return x.Options
	}
	return nil
}

func (x *DeleteStateEnvelope) GetEtag() string {
	if x != nil {
		return x.Etag
	}
	return ""
}

type SaveStateEnvelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      string               `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value    *anypb.Any           `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Etag     string               `protobuf:"bytes,3,opt,name=etag,proto3" json:"etag,omitempty"`
	Metadata map[string]string    `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Options  *StateRequestOptions `protobuf:"bytes,5,opt,name=options,proto3" json:"options,omitempty"`
}

func (x *SaveStateEnvelope) Reset() {
	*x = SaveStateEnvelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SaveStateEnvelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SaveStateEnvelope) ProtoMessage() {}

func (x *SaveStateEnvelope) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SaveStateEnvelope.ProtoReflect.Descriptor instead.
func (*SaveStateEnvelope) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{1}
}

func (x *SaveStateEnvelope) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *SaveStateEnvelope) GetValue() *anypb.Any {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *SaveStateEnvelope) GetEtag() string {
	if x != nil {
		return x.Etag
	}
	return ""
}

func (x *SaveStateEnvelope) GetMetadata() map[string]string {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *SaveStateEnvelope) GetOptions() *StateRequestOptions {
	if x != nil {
		return x.Options
	}
	return nil
}

type GetStateEnvelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key  string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Etag string `protobuf:"bytes,2,opt,name=etag,proto3" json:"etag,omitempty"`
}

func (x *GetStateEnvelope) Reset() {
	*x = GetStateEnvelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetStateEnvelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetStateEnvelope) ProtoMessage() {}

func (x *GetStateEnvelope) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetStateEnvelope.ProtoReflect.Descriptor instead.
func (*GetStateEnvelope) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{2}
}

func (x *GetStateEnvelope) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *GetStateEnvelope) GetEtag() string {
	if x != nil {
		return x.Etag
	}
	return ""
}

type GetStateResponseEnvelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data *anypb.Any `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	Etag string     `protobuf:"bytes,2,opt,name=etag,proto3" json:"etag,omitempty"`
}

func (x *GetStateResponseEnvelope) Reset() {
	*x = GetStateResponseEnvelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetStateResponseEnvelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetStateResponseEnvelope) ProtoMessage() {}

func (x *GetStateResponseEnvelope) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetStateResponseEnvelope.ProtoReflect.Descriptor instead.
func (*GetStateResponseEnvelope) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{3}
}

func (x *GetStateResponseEnvelope) GetData() *anypb.Any {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *GetStateResponseEnvelope) GetEtag() string {
	if x != nil {
		return x.Etag
	}
	return ""
}

type StateOptions struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Concurrency string `protobuf:"bytes,1,opt,name=concurrency,proto3" json:"concurrency,omitempty"`
	Consistency string `protobuf:"bytes,2,opt,name=consistency,proto3" json:"consistency,omitempty"`
}

func (x *StateOptions) Reset() {
	*x = StateOptions{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StateOptions) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StateOptions) ProtoMessage() {}

func (x *StateOptions) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StateOptions.ProtoReflect.Descriptor instead.
func (*StateOptions) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{4}
}

func (x *StateOptions) GetConcurrency() string {
	if x != nil {
		return x.Concurrency
	}
	return ""
}

func (x *StateOptions) GetConsistency() string {
	if x != nil {
		return x.Consistency
	}
	return ""
}

type StateRequestOptions struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Concurrency string `protobuf:"bytes,1,opt,name=concurrency,proto3" json:"concurrency,omitempty"`
	Consistency string `protobuf:"bytes,2,opt,name=consistency,proto3" json:"consistency,omitempty"`
}

func (x *StateRequestOptions) Reset() {
	*x = StateRequestOptions{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StateRequestOptions) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StateRequestOptions) ProtoMessage() {}

func (x *StateRequestOptions) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StateRequestOptions.ProtoReflect.Descriptor instead.
func (*StateRequestOptions) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{5}
}

func (x *StateRequestOptions) GetConcurrency() string {
	if x != nil {
		return x.Concurrency
	}
	return ""
}

func (x *StateRequestOptions) GetConsistency() string {
	if x != nil {
		return x.Consistency
	}
	return ""
}

var File_kv_store_proto protoreflect.FileDescriptor

var file_kv_store_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x6b, 0x76, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x0a, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74, 0x61, 0x74, 0x65, 0x1a, 0x19, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x10, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x5f, 0x6b, 0x65, 0x79,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x75, 0x0a, 0x13, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x12, 0x16, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x04, 0x90, 0xb5, 0x18, 0x01,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x32, 0x0a, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x52, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x65, 0x74, 0x61,
	0x67, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x65, 0x74, 0x61, 0x67, 0x22, 0xac, 0x02,
	0x0a, 0x11, 0x53, 0x61, 0x76, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76, 0x65, 0x6c,
	0x6f, 0x70, 0x65, 0x12, 0x16, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x42, 0x04, 0x90, 0xb5, 0x18, 0x01, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2a, 0x0a, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79,
	0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x65, 0x74, 0x61, 0x67, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x65, 0x74, 0x61, 0x67, 0x12, 0x47, 0x0a, 0x08, 0x6d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2b, 0x2e,
	0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2e, 0x53, 0x61, 0x76, 0x65, 0x53,
	0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x2e, 0x4d, 0x65, 0x74,
	0x61, 0x64, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61,
	0x64, 0x61, 0x74, 0x61, 0x12, 0x39, 0x0a, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1f, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74, 0x61,
	0x74, 0x65, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x4f,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x1a,
	0x3b, 0x0a, 0x0d, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x3e, 0x0a, 0x10,
	0x47, 0x65, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65,
	0x12, 0x16, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x04, 0x90,
	0xb5, 0x18, 0x01, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x65, 0x74, 0x61, 0x67,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x65, 0x74, 0x61, 0x67, 0x22, 0x58, 0x0a, 0x18,
	0x47, 0x65, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x12, 0x28, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x04, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x12, 0x0a, 0x04, 0x65, 0x74, 0x61, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x65, 0x74, 0x61, 0x67, 0x22, 0x52, 0x0a, 0x0c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x4f,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x20, 0x0a, 0x0b, 0x63, 0x6f, 0x6e, 0x63, 0x75, 0x72,
	0x72, 0x65, 0x6e, 0x63, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e,
	0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x63, 0x79, 0x12, 0x20, 0x0a, 0x0b, 0x63, 0x6f, 0x6e, 0x73,
	0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63,
	0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x22, 0x59, 0x0a, 0x13, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x12, 0x20, 0x0a, 0x0b, 0x63, 0x6f, 0x6e, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x63, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x63, 0x75, 0x72, 0x72, 0x65,
	0x6e, 0x63, 0x79, 0x12, 0x20, 0x0a, 0x0b, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e,
	0x63, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73,
	0x74, 0x65, 0x6e, 0x63, 0x79, 0x32, 0xf1, 0x01, 0x0a, 0x0d, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x53, 0x74, 0x6f, 0x72, 0x65, 0x12, 0x50, 0x0a, 0x08, 0x47, 0x65, 0x74, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x12, 0x1c, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74, 0x61, 0x74, 0x65,
	0x2e, 0x47, 0x65, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70,
	0x65, 0x1a, 0x24, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2e, 0x47,
	0x65, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x45,
	0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x22, 0x00, 0x12, 0x44, 0x0a, 0x09, 0x53, 0x61, 0x76,
	0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x1d, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x2e, 0x53, 0x61, 0x76, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76,
	0x65, 0x6c, 0x6f, 0x70, 0x65, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x12,
	0x48, 0x0a, 0x0b, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x1f,
	0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2e, 0x44, 0x65, 0x6c, 0x65,
	0x74, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x1a,
	0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x42, 0x15, 0x5a, 0x13, 0x63, 0x6c, 0x6f,
	0x75, 0x64, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2f, 0x6b, 0x76, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x65,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_kv_store_proto_rawDescOnce sync.Once
	file_kv_store_proto_rawDescData = file_kv_store_proto_rawDesc
)

func file_kv_store_proto_rawDescGZIP() []byte {
	file_kv_store_proto_rawDescOnce.Do(func() {
		file_kv_store_proto_rawDescData = protoimpl.X.CompressGZIP(file_kv_store_proto_rawDescData)
	})
	return file_kv_store_proto_rawDescData
}

var file_kv_store_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_kv_store_proto_goTypes = []interface{}{
	(*DeleteStateEnvelope)(nil),      // 0: cloudstate.DeleteStateEnvelope
	(*SaveStateEnvelope)(nil),        // 1: cloudstate.SaveStateEnvelope
	(*GetStateEnvelope)(nil),         // 2: cloudstate.GetStateEnvelope
	(*GetStateResponseEnvelope)(nil), // 3: cloudstate.GetStateResponseEnvelope
	(*StateOptions)(nil),             // 4: cloudstate.StateOptions
	(*StateRequestOptions)(nil),      // 5: cloudstate.StateRequestOptions
	nil,                              // 6: cloudstate.SaveStateEnvelope.MetadataEntry
	(*anypb.Any)(nil),                // 7: google.protobuf.Any
	(*emptypb.Empty)(nil),            // 8: google.protobuf.Empty
}
var file_kv_store_proto_depIdxs = []int32{
	4, // 0: cloudstate.DeleteStateEnvelope.options:type_name -> cloudstate.StateOptions
	7, // 1: cloudstate.SaveStateEnvelope.value:type_name -> google.protobuf.Any
	6, // 2: cloudstate.SaveStateEnvelope.metadata:type_name -> cloudstate.SaveStateEnvelope.MetadataEntry
	5, // 3: cloudstate.SaveStateEnvelope.options:type_name -> cloudstate.StateRequestOptions
	7, // 4: cloudstate.GetStateResponseEnvelope.data:type_name -> google.protobuf.Any
	2, // 5: cloudstate.KeyValueStore.GetState:input_type -> cloudstate.GetStateEnvelope
	1, // 6: cloudstate.KeyValueStore.SaveState:input_type -> cloudstate.SaveStateEnvelope
	0, // 7: cloudstate.KeyValueStore.DeleteState:input_type -> cloudstate.DeleteStateEnvelope
	3, // 8: cloudstate.KeyValueStore.GetState:output_type -> cloudstate.GetStateResponseEnvelope
	8, // 9: cloudstate.KeyValueStore.SaveState:output_type -> google.protobuf.Empty
	8, // 10: cloudstate.KeyValueStore.DeleteState:output_type -> google.protobuf.Empty
	8, // [8:11] is the sub-list for method output_type
	5, // [5:8] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_kv_store_proto_init() }
func file_kv_store_proto_init() {
	if File_kv_store_proto != nil {
		return
	}
	file_entity_key_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_kv_store_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteStateEnvelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kv_store_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SaveStateEnvelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kv_store_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetStateEnvelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kv_store_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetStateResponseEnvelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kv_store_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StateOptions); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kv_store_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StateRequestOptions); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kv_store_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_kv_store_proto_goTypes,
		DependencyIndexes: file_kv_store_proto_depIdxs,
		MessageInfos:      file_kv_store_proto_msgTypes,
	}.Build()
	File_kv_store_proto = out.File
	file_kv_store_proto_rawDesc = nil
	file_kv_store_proto_goTypes = nil
	file_kv_store_proto_depIdxs = nil
}
