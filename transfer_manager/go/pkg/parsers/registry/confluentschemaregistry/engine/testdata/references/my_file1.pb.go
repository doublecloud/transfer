// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v3.21.3
// source: transfer_manager/go/pkg/parsers/registry/confluentschemaregistry/engine/testdata/references/my_file1.proto

package references

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type MyMsg1 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Msg1StringField string `protobuf:"bytes,1,opt,name=msg1_string_field,json=msg1StringField,proto3" json:"msg1_string_field,omitempty"`
}

func (x *MyMsg1) Reset() {
	*x = MyMsg1{}
	if protoimpl.UnsafeEnabled {
		mi := &file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MyMsg1) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MyMsg1) ProtoMessage() {}

func (x *MyMsg1) ProtoReflect() protoreflect.Message {
	mi := &file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MyMsg1.ProtoReflect.Descriptor instead.
func (*MyMsg1) Descriptor() ([]byte, []int) {
	return file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescGZIP(), []int{0}
}

func (x *MyMsg1) GetMsg1StringField() string {
	if x != nil {
		return x.Msg1StringField
	}
	return ""
}

var File_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto protoreflect.FileDescriptor

var file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDesc = []byte{
	0x0a, 0x6a, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x66, 0x65, 0x72, 0x5f, 0x6d, 0x61, 0x6e, 0x61, 0x67,
	0x65, 0x72, 0x2f, 0x67, 0x6f, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x61, 0x72, 0x73, 0x65, 0x72,
	0x73, 0x2f, 0x72, 0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x6c,
	0x75, 0x65, 0x6e, 0x74, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x72, 0x65, 0x67, 0x69, 0x73, 0x74,
	0x72, 0x79, 0x2f, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x64, 0x61,
	0x74, 0x61, 0x2f, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x73, 0x2f, 0x6d, 0x79,
	0x5f, 0x66, 0x69, 0x6c, 0x65, 0x31, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x35, 0x0a, 0x07,
	0x6d, 0x79, 0x5f, 0x6d, 0x73, 0x67, 0x31, 0x12, 0x2a, 0x0a, 0x11, 0x6d, 0x73, 0x67, 0x31, 0x5f,
	0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x5f, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0f, 0x6d, 0x73, 0x67, 0x31, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x42, 0x6e, 0x5a, 0x6c, 0x61, 0x2e, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2d,
	0x74, 0x65, 0x61, 0x6d, 0x2e, 0x72, 0x75, 0x2f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x66, 0x65, 0x72,
	0x5f, 0x6d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72, 0x2f, 0x67, 0x6f, 0x2f, 0x70, 0x6b, 0x67, 0x2f,
	0x70, 0x61, 0x72, 0x73, 0x65, 0x72, 0x73, 0x2f, 0x72, 0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79,
	0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61,
	0x72, 0x65, 0x67, 0x69, 0x73, 0x74, 0x72, 0x79, 0x2f, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x2f,
	0x74, 0x65, 0x73, 0x74, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e,
	0x63, 0x65, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescOnce sync.Once
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescData = file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDesc
)

func file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescGZIP() []byte {
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescOnce.Do(func() {
		file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescData = protoimpl.X.CompressGZIP(file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescData)
	})
	return file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDescData
}

var file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_goTypes = []interface{}{
	(*MyMsg1)(nil), // 0: my_msg1
}
var file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() {
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_init()
}
func file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_init() {
	if File_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MyMsg1); i {
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
			RawDescriptor: file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_goTypes,
		DependencyIndexes: file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_depIdxs,
		MessageInfos:      file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_msgTypes,
	}.Build()
	File_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto = out.File
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_rawDesc = nil
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_goTypes = nil
	file_transfer_manager_go_pkg_parsers_registry_confluentschemaregistry_engine_testdata_references_my_file1_proto_depIdxs = nil
}
