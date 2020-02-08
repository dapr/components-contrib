// Code generated by protoc-gen-go. DO NOT EDIT.
// source: entity.proto

package protocol

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	_ "github.com/golang/protobuf/protoc-gen-go/descriptor"
	any "github.com/golang/protobuf/ptypes/any"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// A reply to the sender.
type Reply struct {
	// The reply payload
	Payload              *any.Any `protobuf:"bytes,1,opt,name=payload,proto3" json:"payload,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Reply) Reset()         { *m = Reply{} }
func (m *Reply) String() string { return proto.CompactTextString(m) }
func (*Reply) ProtoMessage()    {}
func (*Reply) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{0}
}

func (m *Reply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Reply.Unmarshal(m, b)
}
func (m *Reply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Reply.Marshal(b, m, deterministic)
}
func (m *Reply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Reply.Merge(m, src)
}
func (m *Reply) XXX_Size() int {
	return xxx_messageInfo_Reply.Size(m)
}
func (m *Reply) XXX_DiscardUnknown() {
	xxx_messageInfo_Reply.DiscardUnknown(m)
}

var xxx_messageInfo_Reply proto.InternalMessageInfo

func (m *Reply) GetPayload() *any.Any {
	if m != nil {
		return m.Payload
	}
	return nil
}

// Forwards handling of this request to another entity.
type Forward struct {
	// The name of the service to forward to.
	ServiceName string `protobuf:"bytes,1,opt,name=service_name,json=serviceName,proto3" json:"service_name,omitempty"`
	// The name of the command.
	CommandName string `protobuf:"bytes,2,opt,name=command_name,json=commandName,proto3" json:"command_name,omitempty"`
	// The payload.
	Payload              *any.Any `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Forward) Reset()         { *m = Forward{} }
func (m *Forward) String() string { return proto.CompactTextString(m) }
func (*Forward) ProtoMessage()    {}
func (*Forward) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{1}
}

func (m *Forward) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Forward.Unmarshal(m, b)
}
func (m *Forward) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Forward.Marshal(b, m, deterministic)
}
func (m *Forward) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Forward.Merge(m, src)
}
func (m *Forward) XXX_Size() int {
	return xxx_messageInfo_Forward.Size(m)
}
func (m *Forward) XXX_DiscardUnknown() {
	xxx_messageInfo_Forward.DiscardUnknown(m)
}

var xxx_messageInfo_Forward proto.InternalMessageInfo

func (m *Forward) GetServiceName() string {
	if m != nil {
		return m.ServiceName
	}
	return ""
}

func (m *Forward) GetCommandName() string {
	if m != nil {
		return m.CommandName
	}
	return ""
}

func (m *Forward) GetPayload() *any.Any {
	if m != nil {
		return m.Payload
	}
	return nil
}

// An action for the client
type ClientAction struct {
	// Types that are valid to be assigned to Action:
	//	*ClientAction_Reply
	//	*ClientAction_Forward
	//	*ClientAction_Failure
	Action               isClientAction_Action `protobuf_oneof:"action"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *ClientAction) Reset()         { *m = ClientAction{} }
func (m *ClientAction) String() string { return proto.CompactTextString(m) }
func (*ClientAction) ProtoMessage()    {}
func (*ClientAction) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{2}
}

func (m *ClientAction) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ClientAction.Unmarshal(m, b)
}
func (m *ClientAction) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ClientAction.Marshal(b, m, deterministic)
}
func (m *ClientAction) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ClientAction.Merge(m, src)
}
func (m *ClientAction) XXX_Size() int {
	return xxx_messageInfo_ClientAction.Size(m)
}
func (m *ClientAction) XXX_DiscardUnknown() {
	xxx_messageInfo_ClientAction.DiscardUnknown(m)
}

var xxx_messageInfo_ClientAction proto.InternalMessageInfo

type isClientAction_Action interface {
	isClientAction_Action()
}

type ClientAction_Reply struct {
	Reply *Reply `protobuf:"bytes,1,opt,name=reply,proto3,oneof"`
}

type ClientAction_Forward struct {
	Forward *Forward `protobuf:"bytes,2,opt,name=forward,proto3,oneof"`
}

type ClientAction_Failure struct {
	Failure *Failure `protobuf:"bytes,3,opt,name=failure,proto3,oneof"`
}

func (*ClientAction_Reply) isClientAction_Action() {}

func (*ClientAction_Forward) isClientAction_Action() {}

func (*ClientAction_Failure) isClientAction_Action() {}

func (m *ClientAction) GetAction() isClientAction_Action {
	if m != nil {
		return m.Action
	}
	return nil
}

func (m *ClientAction) GetReply() *Reply {
	if x, ok := m.GetAction().(*ClientAction_Reply); ok {
		return x.Reply
	}
	return nil
}

func (m *ClientAction) GetForward() *Forward {
	if x, ok := m.GetAction().(*ClientAction_Forward); ok {
		return x.Forward
	}
	return nil
}

func (m *ClientAction) GetFailure() *Failure {
	if x, ok := m.GetAction().(*ClientAction_Failure); ok {
		return x.Failure
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*ClientAction) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*ClientAction_Reply)(nil),
		(*ClientAction_Forward)(nil),
		(*ClientAction_Failure)(nil),
	}
}

// A side effect to be done after this command is handled.
type SideEffect struct {
	// The name of the service to perform the side effect on.
	ServiceName string `protobuf:"bytes,1,opt,name=service_name,json=serviceName,proto3" json:"service_name,omitempty"`
	// The name of the command.
	CommandName string `protobuf:"bytes,2,opt,name=command_name,json=commandName,proto3" json:"command_name,omitempty"`
	// The payload of the command.
	Payload *any.Any `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
	// Whether this side effect should be performed synchronously, ie, before the reply is eventually
	// sent, or not.
	Synchronous          bool     `protobuf:"varint,4,opt,name=synchronous,proto3" json:"synchronous,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SideEffect) Reset()         { *m = SideEffect{} }
func (m *SideEffect) String() string { return proto.CompactTextString(m) }
func (*SideEffect) ProtoMessage()    {}
func (*SideEffect) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{3}
}

func (m *SideEffect) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SideEffect.Unmarshal(m, b)
}
func (m *SideEffect) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SideEffect.Marshal(b, m, deterministic)
}
func (m *SideEffect) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SideEffect.Merge(m, src)
}
func (m *SideEffect) XXX_Size() int {
	return xxx_messageInfo_SideEffect.Size(m)
}
func (m *SideEffect) XXX_DiscardUnknown() {
	xxx_messageInfo_SideEffect.DiscardUnknown(m)
}

var xxx_messageInfo_SideEffect proto.InternalMessageInfo

func (m *SideEffect) GetServiceName() string {
	if m != nil {
		return m.ServiceName
	}
	return ""
}

func (m *SideEffect) GetCommandName() string {
	if m != nil {
		return m.CommandName
	}
	return ""
}

func (m *SideEffect) GetPayload() *any.Any {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (m *SideEffect) GetSynchronous() bool {
	if m != nil {
		return m.Synchronous
	}
	return false
}

// A command. For each command received, a reply must be sent with a matching command id.
type Command struct {
	// The ID of the entity.
	EntityId string `protobuf:"bytes,1,opt,name=entity_id,json=entityId,proto3" json:"entity_id,omitempty"`
	// A command id.
	Id int64 `protobuf:"varint,2,opt,name=id,proto3" json:"id,omitempty"`
	// Command name
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	// The command payload.
	Payload *any.Any `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
	// Whether the command is streamed or not
	Streamed             bool     `protobuf:"varint,5,opt,name=streamed,proto3" json:"streamed,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Command) Reset()         { *m = Command{} }
func (m *Command) String() string { return proto.CompactTextString(m) }
func (*Command) ProtoMessage()    {}
func (*Command) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{4}
}

func (m *Command) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Command.Unmarshal(m, b)
}
func (m *Command) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Command.Marshal(b, m, deterministic)
}
func (m *Command) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Command.Merge(m, src)
}
func (m *Command) XXX_Size() int {
	return xxx_messageInfo_Command.Size(m)
}
func (m *Command) XXX_DiscardUnknown() {
	xxx_messageInfo_Command.DiscardUnknown(m)
}

var xxx_messageInfo_Command proto.InternalMessageInfo

func (m *Command) GetEntityId() string {
	if m != nil {
		return m.EntityId
	}
	return ""
}

func (m *Command) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Command) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Command) GetPayload() *any.Any {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (m *Command) GetStreamed() bool {
	if m != nil {
		return m.Streamed
	}
	return false
}

type StreamCancelled struct {
	// The ID of the entity
	EntityId string `protobuf:"bytes,1,opt,name=entity_id,json=entityId,proto3" json:"entity_id,omitempty"`
	// The command id
	Id                   int64    `protobuf:"varint,2,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *StreamCancelled) Reset()         { *m = StreamCancelled{} }
func (m *StreamCancelled) String() string { return proto.CompactTextString(m) }
func (*StreamCancelled) ProtoMessage()    {}
func (*StreamCancelled) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{5}
}

func (m *StreamCancelled) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StreamCancelled.Unmarshal(m, b)
}
func (m *StreamCancelled) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StreamCancelled.Marshal(b, m, deterministic)
}
func (m *StreamCancelled) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamCancelled.Merge(m, src)
}
func (m *StreamCancelled) XXX_Size() int {
	return xxx_messageInfo_StreamCancelled.Size(m)
}
func (m *StreamCancelled) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamCancelled.DiscardUnknown(m)
}

var xxx_messageInfo_StreamCancelled proto.InternalMessageInfo

func (m *StreamCancelled) GetEntityId() string {
	if m != nil {
		return m.EntityId
	}
	return ""
}

func (m *StreamCancelled) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

// A failure reply. If this is returned, it will be translated into a gRPC unknown
// error with the corresponding description if supplied.
type Failure struct {
	// The id of the command being replied to. Must match the input command.
	CommandId int64 `protobuf:"varint,1,opt,name=command_id,json=commandId,proto3" json:"command_id,omitempty"`
	// A description of the error.
	Description          string   `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Failure) Reset()         { *m = Failure{} }
func (m *Failure) String() string { return proto.CompactTextString(m) }
func (*Failure) ProtoMessage()    {}
func (*Failure) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{6}
}

func (m *Failure) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Failure.Unmarshal(m, b)
}
func (m *Failure) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Failure.Marshal(b, m, deterministic)
}
func (m *Failure) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Failure.Merge(m, src)
}
func (m *Failure) XXX_Size() int {
	return xxx_messageInfo_Failure.Size(m)
}
func (m *Failure) XXX_DiscardUnknown() {
	xxx_messageInfo_Failure.DiscardUnknown(m)
}

var xxx_messageInfo_Failure proto.InternalMessageInfo

func (m *Failure) GetCommandId() int64 {
	if m != nil {
		return m.CommandId
	}
	return 0
}

func (m *Failure) GetDescription() string {
	if m != nil {
		return m.Description
	}
	return ""
}

type EntitySpec struct {
	// This should be the Descriptors.FileDescriptorSet in proto serialized from as generated by:
	// protoc --include_imports \
	// --proto_path=<proto file directory> \
	// --descriptor_set_out=user-function.desc \
	// <path to .proto files>
	Proto []byte `protobuf:"bytes,1,opt,name=proto,proto3" json:"proto,omitempty"`
	// The entities being served.
	Entities []*Entity `protobuf:"bytes,2,rep,name=entities,proto3" json:"entities,omitempty"`
	// Optional information about the service.
	ServiceInfo          *ServiceInfo `protobuf:"bytes,3,opt,name=service_info,json=serviceInfo,proto3" json:"service_info,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *EntitySpec) Reset()         { *m = EntitySpec{} }
func (m *EntitySpec) String() string { return proto.CompactTextString(m) }
func (*EntitySpec) ProtoMessage()    {}
func (*EntitySpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{7}
}

func (m *EntitySpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_EntitySpec.Unmarshal(m, b)
}
func (m *EntitySpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_EntitySpec.Marshal(b, m, deterministic)
}
func (m *EntitySpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_EntitySpec.Merge(m, src)
}
func (m *EntitySpec) XXX_Size() int {
	return xxx_messageInfo_EntitySpec.Size(m)
}
func (m *EntitySpec) XXX_DiscardUnknown() {
	xxx_messageInfo_EntitySpec.DiscardUnknown(m)
}

var xxx_messageInfo_EntitySpec proto.InternalMessageInfo

func (m *EntitySpec) GetProto() []byte {
	if m != nil {
		return m.Proto
	}
	return nil
}

func (m *EntitySpec) GetEntities() []*Entity {
	if m != nil {
		return m.Entities
	}
	return nil
}

func (m *EntitySpec) GetServiceInfo() *ServiceInfo {
	if m != nil {
		return m.ServiceInfo
	}
	return nil
}

// Information about the service that proxy is proxying to.
// All of the information in here is optional. It may be useful for debug purposes.
type ServiceInfo struct {
	// The name of the service, eg, "shopping-cart".
	ServiceName string `protobuf:"bytes,1,opt,name=service_name,json=serviceName,proto3" json:"service_name,omitempty"`
	// The version of the service.
	ServiceVersion string `protobuf:"bytes,2,opt,name=service_version,json=serviceVersion,proto3" json:"service_version,omitempty"`
	// A description of the runtime for the service. Can be anything, but examples might be:
	// - node v10.15.2
	// - OpenJDK Runtime Environment 1.8.0_192-b12
	ServiceRuntime string `protobuf:"bytes,3,opt,name=service_runtime,json=serviceRuntime,proto3" json:"service_runtime,omitempty"`
	// If using a support library, the name of that library, eg "cloudstate"
	SupportLibraryName string `protobuf:"bytes,4,opt,name=support_library_name,json=supportLibraryName,proto3" json:"support_library_name,omitempty"`
	// The version of the support library being used.
	SupportLibraryVersion string   `protobuf:"bytes,5,opt,name=support_library_version,json=supportLibraryVersion,proto3" json:"support_library_version,omitempty"`
	XXX_NoUnkeyedLiteral  struct{} `json:"-"`
	XXX_unrecognized      []byte   `json:"-"`
	XXX_sizecache         int32    `json:"-"`
}

func (m *ServiceInfo) Reset()         { *m = ServiceInfo{} }
func (m *ServiceInfo) String() string { return proto.CompactTextString(m) }
func (*ServiceInfo) ProtoMessage()    {}
func (*ServiceInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{8}
}

func (m *ServiceInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ServiceInfo.Unmarshal(m, b)
}
func (m *ServiceInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ServiceInfo.Marshal(b, m, deterministic)
}
func (m *ServiceInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ServiceInfo.Merge(m, src)
}
func (m *ServiceInfo) XXX_Size() int {
	return xxx_messageInfo_ServiceInfo.Size(m)
}
func (m *ServiceInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_ServiceInfo.DiscardUnknown(m)
}

var xxx_messageInfo_ServiceInfo proto.InternalMessageInfo

func (m *ServiceInfo) GetServiceName() string {
	if m != nil {
		return m.ServiceName
	}
	return ""
}

func (m *ServiceInfo) GetServiceVersion() string {
	if m != nil {
		return m.ServiceVersion
	}
	return ""
}

func (m *ServiceInfo) GetServiceRuntime() string {
	if m != nil {
		return m.ServiceRuntime
	}
	return ""
}

func (m *ServiceInfo) GetSupportLibraryName() string {
	if m != nil {
		return m.SupportLibraryName
	}
	return ""
}

func (m *ServiceInfo) GetSupportLibraryVersion() string {
	if m != nil {
		return m.SupportLibraryVersion
	}
	return ""
}

type Entity struct {
	// The type of entity. By convention, this should be a fully qualified entity protocol grpc
	// service name, for example, cloudstate.eventsourced.EventSourced.
	EntityType string `protobuf:"bytes,1,opt,name=entity_type,json=entityType,proto3" json:"entity_type,omitempty"`
	// The name of the service to load from the protobuf file.
	ServiceName string `protobuf:"bytes,2,opt,name=service_name,json=serviceName,proto3" json:"service_name,omitempty"`
	// The ID to namespace state by. How this is used depends on the type of entity, for example,
	// event sourced entities will prefix this to the persistence id.
	PersistenceId        string   `protobuf:"bytes,3,opt,name=persistence_id,json=persistenceId,proto3" json:"persistence_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Entity) Reset()         { *m = Entity{} }
func (m *Entity) String() string { return proto.CompactTextString(m) }
func (*Entity) ProtoMessage()    {}
func (*Entity) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{9}
}

func (m *Entity) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Entity.Unmarshal(m, b)
}
func (m *Entity) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Entity.Marshal(b, m, deterministic)
}
func (m *Entity) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Entity.Merge(m, src)
}
func (m *Entity) XXX_Size() int {
	return xxx_messageInfo_Entity.Size(m)
}
func (m *Entity) XXX_DiscardUnknown() {
	xxx_messageInfo_Entity.DiscardUnknown(m)
}

var xxx_messageInfo_Entity proto.InternalMessageInfo

func (m *Entity) GetEntityType() string {
	if m != nil {
		return m.EntityType
	}
	return ""
}

func (m *Entity) GetServiceName() string {
	if m != nil {
		return m.ServiceName
	}
	return ""
}

func (m *Entity) GetPersistenceId() string {
	if m != nil {
		return m.PersistenceId
	}
	return ""
}

type UserFunctionError struct {
	Message              string   `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *UserFunctionError) Reset()         { *m = UserFunctionError{} }
func (m *UserFunctionError) String() string { return proto.CompactTextString(m) }
func (*UserFunctionError) ProtoMessage()    {}
func (*UserFunctionError) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{10}
}

func (m *UserFunctionError) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_UserFunctionError.Unmarshal(m, b)
}
func (m *UserFunctionError) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_UserFunctionError.Marshal(b, m, deterministic)
}
func (m *UserFunctionError) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UserFunctionError.Merge(m, src)
}
func (m *UserFunctionError) XXX_Size() int {
	return xxx_messageInfo_UserFunctionError.Size(m)
}
func (m *UserFunctionError) XXX_DiscardUnknown() {
	xxx_messageInfo_UserFunctionError.DiscardUnknown(m)
}

var xxx_messageInfo_UserFunctionError proto.InternalMessageInfo

func (m *UserFunctionError) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

type ProxyInfo struct {
	ProtocolMajorVersion int32    `protobuf:"varint,1,opt,name=protocol_major_version,json=protocolMajorVersion,proto3" json:"protocol_major_version,omitempty"`
	ProtocolMinorVersion int32    `protobuf:"varint,2,opt,name=protocol_minor_version,json=protocolMinorVersion,proto3" json:"protocol_minor_version,omitempty"`
	ProxyName            string   `protobuf:"bytes,3,opt,name=proxy_name,json=proxyName,proto3" json:"proxy_name,omitempty"`
	ProxyVersion         string   `protobuf:"bytes,4,opt,name=proxy_version,json=proxyVersion,proto3" json:"proxy_version,omitempty"`
	SupportedEntityTypes []string `protobuf:"bytes,5,rep,name=supported_entity_types,json=supportedEntityTypes,proto3" json:"supported_entity_types,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProxyInfo) Reset()         { *m = ProxyInfo{} }
func (m *ProxyInfo) String() string { return proto.CompactTextString(m) }
func (*ProxyInfo) ProtoMessage()    {}
func (*ProxyInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_cf50d946d740d100, []int{11}
}

func (m *ProxyInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProxyInfo.Unmarshal(m, b)
}
func (m *ProxyInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProxyInfo.Marshal(b, m, deterministic)
}
func (m *ProxyInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProxyInfo.Merge(m, src)
}
func (m *ProxyInfo) XXX_Size() int {
	return xxx_messageInfo_ProxyInfo.Size(m)
}
func (m *ProxyInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_ProxyInfo.DiscardUnknown(m)
}

var xxx_messageInfo_ProxyInfo proto.InternalMessageInfo

func (m *ProxyInfo) GetProtocolMajorVersion() int32 {
	if m != nil {
		return m.ProtocolMajorVersion
	}
	return 0
}

func (m *ProxyInfo) GetProtocolMinorVersion() int32 {
	if m != nil {
		return m.ProtocolMinorVersion
	}
	return 0
}

func (m *ProxyInfo) GetProxyName() string {
	if m != nil {
		return m.ProxyName
	}
	return ""
}

func (m *ProxyInfo) GetProxyVersion() string {
	if m != nil {
		return m.ProxyVersion
	}
	return ""
}

func (m *ProxyInfo) GetSupportedEntityTypes() []string {
	if m != nil {
		return m.SupportedEntityTypes
	}
	return nil
}

func init() {
	proto.RegisterType((*Reply)(nil), "cloudstate.Reply")
	proto.RegisterType((*Forward)(nil), "cloudstate.Forward")
	proto.RegisterType((*ClientAction)(nil), "cloudstate.ClientAction")
	proto.RegisterType((*SideEffect)(nil), "cloudstate.SideEffect")
	proto.RegisterType((*Command)(nil), "cloudstate.Command")
	proto.RegisterType((*StreamCancelled)(nil), "cloudstate.StreamCancelled")
	proto.RegisterType((*Failure)(nil), "cloudstate.Failure")
	proto.RegisterType((*EntitySpec)(nil), "cloudstate.EntitySpec")
	proto.RegisterType((*ServiceInfo)(nil), "cloudstate.ServiceInfo")
	proto.RegisterType((*Entity)(nil), "cloudstate.Entity")
	proto.RegisterType((*UserFunctionError)(nil), "cloudstate.UserFunctionError")
	proto.RegisterType((*ProxyInfo)(nil), "cloudstate.ProxyInfo")
}

func init() { proto.RegisterFile("entity.proto", fileDescriptor_cf50d946d740d100) }

var fileDescriptor_cf50d946d740d100 = []byte{
	// 792 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xc4, 0x55, 0xcd, 0x6e, 0xf3, 0x44,
	0x14, 0x8d, 0xf3, 0xd3, 0x24, 0xd7, 0x69, 0xab, 0x4e, 0xd3, 0x34, 0xa4, 0xaa, 0x08, 0x46, 0x88,
	0xb0, 0xa8, 0x83, 0x02, 0x02, 0x09, 0x24, 0xa4, 0xb6, 0xa4, 0x6a, 0x10, 0x20, 0xe4, 0x00, 0x0b,
	0x36, 0x91, 0x6b, 0x4f, 0xca, 0x20, 0x7b, 0xc6, 0x9a, 0x71, 0x0a, 0x5e, 0xf1, 0x06, 0x2c, 0xfb,
	0x04, 0xf0, 0x76, 0x6c, 0x78, 0x03, 0xe4, 0xf9, 0x71, 0xa6, 0xc9, 0xa6, 0xdf, 0xea, 0xdb, 0x79,
	0xce, 0x3d, 0x67, 0xe6, 0xdc, 0x33, 0x77, 0x12, 0xe8, 0x61, 0x9a, 0x93, 0xbc, 0xf0, 0x33, 0xce,
	0x72, 0x86, 0x20, 0x4a, 0xd8, 0x26, 0x16, 0x79, 0x98, 0xe3, 0xd1, 0x3b, 0x8f, 0x8c, 0x3d, 0x26,
	0x78, 0x2a, 0x2b, 0x0f, 0x9b, 0xf5, 0x34, 0xa4, 0x9a, 0x36, 0xba, 0xd8, 0x2d, 0xe1, 0x34, 0x33,
	0x7b, 0x8c, 0xc6, 0xbb, 0xc5, 0x18, 0x8b, 0x88, 0x93, 0x2c, 0x67, 0x5c, 0x31, 0xbc, 0xcf, 0xa1,
	0x15, 0xe0, 0x2c, 0x29, 0x90, 0x0f, 0xed, 0x2c, 0x2c, 0x12, 0x16, 0xc6, 0x43, 0x67, 0xec, 0x4c,
	0xdc, 0x59, 0xdf, 0x57, 0x62, 0xdf, 0x88, 0xfd, 0x6b, 0x5a, 0x04, 0x86, 0xe4, 0xfd, 0x09, 0xed,
	0x3b, 0xc6, 0x7f, 0x0f, 0x79, 0x8c, 0xde, 0x83, 0x9e, 0xc0, 0xfc, 0x89, 0x44, 0x78, 0x45, 0xc3,
	0x14, 0x4b, 0x7d, 0x37, 0x70, 0x35, 0xf6, 0x7d, 0x98, 0xe2, 0x92, 0x12, 0xb1, 0x34, 0x0d, 0x69,
	0xac, 0x28, 0x75, 0x45, 0xd1, 0x98, 0xa4, 0x58, 0x06, 0x1a, 0xaf, 0x31, 0xf0, 0x8f, 0x03, 0xbd,
	0xdb, 0x84, 0x60, 0x9a, 0x5f, 0x47, 0x39, 0x61, 0x14, 0x7d, 0x04, 0x2d, 0x5e, 0xb6, 0xa2, 0xfd,
	0x9f, 0xf8, 0xdb, 0x00, 0x7d, 0xd9, 0xe3, 0x7d, 0x2d, 0x50, 0x0c, 0x34, 0x85, 0xf6, 0x5a, 0x99,
	0x97, 0x4e, 0xdc, 0xd9, 0xa9, 0x4d, 0xd6, 0x7d, 0xdd, 0xd7, 0x02, 0xc3, 0x92, 0x82, 0x90, 0x24,
	0x1b, 0x8e, 0xb5, 0xb9, 0x97, 0x02, 0x55, 0x92, 0x02, 0xf5, 0x79, 0xd3, 0x81, 0x83, 0x50, 0xda,
	0xf2, 0xfe, 0x76, 0x00, 0x96, 0x24, 0xc6, 0xf3, 0xf5, 0x1a, 0x47, 0xf9, 0xdb, 0x09, 0x0b, 0x8d,
	0xc1, 0x15, 0x05, 0x8d, 0x7e, 0xe5, 0x8c, 0xb2, 0x8d, 0x18, 0x36, 0xc7, 0xce, 0xa4, 0x13, 0xd8,
	0x90, 0xf7, 0xec, 0x40, 0xfb, 0x56, 0x9d, 0x80, 0x2e, 0xa0, 0xab, 0x46, 0x71, 0x45, 0x62, 0x6d,
	0xb0, 0xa3, 0x80, 0x45, 0x8c, 0x8e, 0xa0, 0x4e, 0x54, 0x6c, 0x8d, 0xa0, 0x4e, 0x62, 0x84, 0xa0,
	0x29, 0x5d, 0x36, 0x24, 0x4f, 0x7e, 0xdb, 0xf6, 0x9a, 0xaf, 0xb1, 0x37, 0x82, 0x8e, 0xc8, 0x39,
	0x0e, 0x53, 0x1c, 0x0f, 0x5b, 0xd2, 0x5b, 0xb5, 0xf6, 0xbe, 0x82, 0xe3, 0xa5, 0xfc, 0xbe, 0x0d,
	0x69, 0x84, 0x93, 0x04, 0xbf, 0x99, 0x3f, 0xef, 0x1b, 0x68, 0xeb, 0xfb, 0x41, 0x97, 0x00, 0x26,
	0x58, 0x2d, 0x6c, 0x04, 0x5d, 0x8d, 0x2c, 0x64, 0x48, 0xe6, 0x7d, 0x10, 0x46, 0x4d, 0xec, 0x16,
	0xe4, 0xfd, 0xe5, 0x00, 0xcc, 0xe5, 0x41, 0xcb, 0x0c, 0x47, 0xa8, 0x0f, 0x2d, 0xd9, 0x8f, 0xdc,
	0xaa, 0x17, 0xa8, 0x05, 0xf2, 0x41, 0x99, 0x21, 0x58, 0x0c, 0xeb, 0xe3, 0xc6, 0xc4, 0x9d, 0x21,
	0x7b, 0x58, 0x94, 0x3e, 0xa8, 0x38, 0xe8, 0x8b, 0xed, 0x44, 0x10, 0xba, 0x66, 0xfa, 0x42, 0xcf,
	0x6d, 0xcd, 0x52, 0xd5, 0x17, 0x74, 0xcd, 0xaa, 0x51, 0x29, 0x17, 0xde, 0xbf, 0x0e, 0xb8, 0x56,
	0xf1, 0x35, 0xd3, 0xf5, 0x21, 0x1c, 0x1b, 0xca, 0x13, 0xe6, 0x62, 0xdb, 0xe9, 0x91, 0x86, 0x7f,
	0x56, 0xa8, 0x4d, 0xe4, 0x1b, 0x9a, 0x93, 0xea, 0x8e, 0x0d, 0x31, 0x50, 0x28, 0xfa, 0x18, 0xfa,
	0x62, 0x93, 0x65, 0x8c, 0xe7, 0xab, 0x84, 0x3c, 0xf0, 0x90, 0x17, 0xea, 0xf0, 0xa6, 0x64, 0x23,
	0x5d, 0xfb, 0x56, 0x95, 0xa4, 0x87, 0xcf, 0xe0, 0x7c, 0x57, 0x61, 0xbc, 0xb4, 0xa4, 0xe8, 0xec,
	0xa5, 0x48, 0x5b, 0xf2, 0x04, 0x1c, 0xa8, 0xf8, 0xd0, 0xbb, 0xe0, 0xea, 0x11, 0xc8, 0x8b, 0xcc,
	0xf4, 0x09, 0x0a, 0xfa, 0xb1, 0xc8, 0xf0, 0x5e, 0x12, 0xf5, 0xfd, 0x24, 0x3e, 0x80, 0xa3, 0xac,
	0xdc, 0x58, 0xe4, 0x98, 0x96, 0xe1, 0xc7, 0xba, 0xbf, 0x43, 0x0b, 0x5d, 0xc4, 0xde, 0x15, 0x9c,
	0xfc, 0x24, 0x30, 0xbf, 0xdb, 0x50, 0xf9, 0xa0, 0xe7, 0x9c, 0x33, 0x8e, 0x86, 0xd0, 0x4e, 0xb1,
	0x10, 0xe1, 0xa3, 0x39, 0xdb, 0x2c, 0xbd, 0xff, 0x1c, 0xe8, 0xfe, 0xc0, 0xd9, 0x1f, 0x85, 0xbc,
	0x90, 0x4f, 0x61, 0x20, 0xa7, 0x22, 0x62, 0xc9, 0x2a, 0x0d, 0x7f, 0x63, 0xbc, 0x6a, 0xb4, 0x94,
	0xb5, 0x82, 0xbe, 0xa9, 0x7e, 0x57, 0x16, 0x4d, 0xf4, 0x2f, 0x54, 0x84, 0x5a, 0xaa, 0xfa, 0x8e,
	0xaa, 0x2c, 0x1a, 0xd5, 0x25, 0x40, 0x56, 0x1e, 0xbc, 0xb2, 0xde, 0x63, 0x57, 0x22, 0xb2, 0xdd,
	0xf7, 0xe1, 0x50, 0x95, 0xcd, 0x5e, 0xea, 0x7e, 0x7a, 0x12, 0xb4, 0x4e, 0xd6, 0xd1, 0xe3, 0x78,
	0x65, 0x25, 0x2c, 0x86, 0xad, 0x71, 0x63, 0xd2, 0x0d, 0xfa, 0x55, 0x75, 0x5e, 0x65, 0x2d, 0x66,
	0xcf, 0x0e, 0x1c, 0xab, 0xf5, 0xd7, 0x44, 0x44, 0xec, 0x09, 0xf3, 0x02, 0x7d, 0x09, 0x9d, 0x58,
	0x2f, 0xd0, 0x99, 0x3d, 0xcc, 0x55, 0x38, 0xa3, 0xc1, 0xfe, 0xbb, 0x28, 0xdf, 0x95, 0x57, 0x43,
	0x77, 0xe0, 0x72, 0x5c, 0x9e, 0xa3, 0xd2, 0xbe, 0xb4, 0x89, 0x7b, 0x97, 0x31, 0x1a, 0xec, 0xfd,
	0xba, 0xcc, 0xcb, 0x3f, 0x41, 0xaf, 0x76, 0x73, 0x05, 0x03, 0xc2, 0x6c, 0xb1, 0x09, 0xee, 0x97,
	0xd3, 0x2d, 0x38, 0x35, 0xe0, 0xc3, 0x81, 0xfc, 0xfa, 0xe4, 0xff, 0x00, 0x00, 0x00, 0xff, 0xff,
	0x72, 0x81, 0xe3, 0x55, 0x8a, 0x07, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// EntityDiscoveryClient is the client API for EntityDiscovery service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type EntityDiscoveryClient interface {
	// Discover what entities the user function wishes to serve.
	Discover(ctx context.Context, in *ProxyInfo, opts ...grpc.CallOption) (*EntitySpec, error)
	// Report an error back to the user function. This will only be invoked to tell the user function
	// that it has done something wrong, eg, violated the protocol, tried to use an entity type that
	// isn't supported, or attempted to forward to an entity that doesn't exist, etc. These messages
	// should be logged clearly for debugging purposes.
	ReportError(ctx context.Context, in *UserFunctionError, opts ...grpc.CallOption) (*empty.Empty, error)
}

type entityDiscoveryClient struct {
	cc *grpc.ClientConn
}

func NewEntityDiscoveryClient(cc *grpc.ClientConn) EntityDiscoveryClient {
	return &entityDiscoveryClient{cc}
}

func (c *entityDiscoveryClient) Discover(ctx context.Context, in *ProxyInfo, opts ...grpc.CallOption) (*EntitySpec, error) {
	out := new(EntitySpec)
	err := c.cc.Invoke(ctx, "/cloudstate.EntityDiscovery/discover", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *entityDiscoveryClient) ReportError(ctx context.Context, in *UserFunctionError, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/cloudstate.EntityDiscovery/reportError", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EntityDiscoveryServer is the server API for EntityDiscovery service.
type EntityDiscoveryServer interface {
	// Discover what entities the user function wishes to serve.
	Discover(context.Context, *ProxyInfo) (*EntitySpec, error)
	// Report an error back to the user function. This will only be invoked to tell the user function
	// that it has done something wrong, eg, violated the protocol, tried to use an entity type that
	// isn't supported, or attempted to forward to an entity that doesn't exist, etc. These messages
	// should be logged clearly for debugging purposes.
	ReportError(context.Context, *UserFunctionError) (*empty.Empty, error)
}

// UnimplementedEntityDiscoveryServer can be embedded to have forward compatible implementations.
type UnimplementedEntityDiscoveryServer struct {
}

func (*UnimplementedEntityDiscoveryServer) Discover(ctx context.Context, req *ProxyInfo) (*EntitySpec, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Discover not implemented")
}
func (*UnimplementedEntityDiscoveryServer) ReportError(ctx context.Context, req *UserFunctionError) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportError not implemented")
}

func RegisterEntityDiscoveryServer(s *grpc.Server, srv EntityDiscoveryServer) {
	s.RegisterService(&_EntityDiscovery_serviceDesc, srv)
}

func _EntityDiscovery_Discover_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProxyInfo)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EntityDiscoveryServer).Discover(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cloudstate.EntityDiscovery/Discover",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EntityDiscoveryServer).Discover(ctx, req.(*ProxyInfo))
	}
	return interceptor(ctx, in, info, handler)
}

func _EntityDiscovery_ReportError_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UserFunctionError)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EntityDiscoveryServer).ReportError(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cloudstate.EntityDiscovery/ReportError",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EntityDiscoveryServer).ReportError(ctx, req.(*UserFunctionError))
	}
	return interceptor(ctx, in, info, handler)
}

var _EntityDiscovery_serviceDesc = grpc.ServiceDesc{
	ServiceName: "cloudstate.EntityDiscovery",
	HandlerType: (*EntityDiscoveryServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "discover",
			Handler:    _EntityDiscovery_Discover_Handler,
		},
		{
			MethodName: "reportError",
			Handler:    _EntityDiscovery_ReportError_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "entity.proto",
}
