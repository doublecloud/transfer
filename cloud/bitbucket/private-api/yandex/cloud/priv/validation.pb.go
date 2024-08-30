// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.22.5
// source: yandex/cloud/priv/validation.proto

package cloud

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type MapKeySpec struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value   string `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Pattern string `protobuf:"bytes,2,opt,name=pattern,proto3" json:"pattern,omitempty"`
	Length  string `protobuf:"bytes,3,opt,name=length,proto3" json:"length,omitempty"`
}

func (x *MapKeySpec) Reset() {
	*x = MapKeySpec{}
	if protoimpl.UnsafeEnabled {
		mi := &file_yandex_cloud_priv_validation_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MapKeySpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MapKeySpec) ProtoMessage() {}

func (x *MapKeySpec) ProtoReflect() protoreflect.Message {
	mi := &file_yandex_cloud_priv_validation_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MapKeySpec.ProtoReflect.Descriptor instead.
func (*MapKeySpec) Descriptor() ([]byte, []int) {
	return file_yandex_cloud_priv_validation_proto_rawDescGZIP(), []int{0}
}

func (x *MapKeySpec) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *MapKeySpec) GetPattern() string {
	if x != nil {
		return x.Pattern
	}
	return ""
}

func (x *MapKeySpec) GetLength() string {
	if x != nil {
		return x.Length
	}
	return ""
}

var file_yandex_cloud_priv_validation_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.OneofOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         100400,
		Name:          "yandex.cloud.priv.exactly_one",
		Tag:           "varint,100400,opt,name=exactly_one",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         100501,
		Name:          "yandex.cloud.priv.required",
		Tag:           "varint,100501,opt,name=required",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*string)(nil),
		Field:         100502,
		Name:          "yandex.cloud.priv.pattern",
		Tag:           "bytes,100502,opt,name=pattern",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*string)(nil),
		Field:         100503,
		Name:          "yandex.cloud.priv.value",
		Tag:           "bytes,100503,opt,name=value",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*string)(nil),
		Field:         100504,
		Name:          "yandex.cloud.priv.size",
		Tag:           "bytes,100504,opt,name=size",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*string)(nil),
		Field:         100505,
		Name:          "yandex.cloud.priv.length",
		Tag:           "bytes,100505,opt,name=length",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         100506,
		Name:          "yandex.cloud.priv.unique",
		Tag:           "varint,100506,opt,name=unique",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*MapKeySpec)(nil),
		Field:         100510,
		Name:          "yandex.cloud.priv.map_key",
		Tag:           "bytes,100510,opt,name=map_key",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*string)(nil),
		Field:         100511,
		Name:          "yandex.cloud.priv.bytes",
		Tag:           "bytes,100511,opt,name=bytes",
		Filename:      "yandex/cloud/priv/validation.proto",
	},
}

// Extension fields to descriptorpb.OneofOptions.
var (
	// optional bool exactly_one = 100400;
	E_ExactlyOne = &file_yandex_cloud_priv_validation_proto_extTypes[0]
)

// Extension fields to descriptorpb.FieldOptions.
var (
	// optional bool required = 100501;
	E_Required = &file_yandex_cloud_priv_validation_proto_extTypes[1]
	// optional string pattern = 100502;
	E_Pattern = &file_yandex_cloud_priv_validation_proto_extTypes[2]
	// optional string value = 100503;
	E_Value = &file_yandex_cloud_priv_validation_proto_extTypes[3]
	// optional string size = 100504;
	E_Size = &file_yandex_cloud_priv_validation_proto_extTypes[4]
	// optional string length = 100505;
	E_Length = &file_yandex_cloud_priv_validation_proto_extTypes[5]
	// optional bool unique = 100506;
	E_Unique = &file_yandex_cloud_priv_validation_proto_extTypes[6]
	// optional yandex.cloud.priv.MapKeySpec map_key = 100510;
	E_MapKey = &file_yandex_cloud_priv_validation_proto_extTypes[7]
	// optional string bytes = 100511;
	E_Bytes = &file_yandex_cloud_priv_validation_proto_extTypes[8]
)

var File_yandex_cloud_priv_validation_proto protoreflect.FileDescriptor

var file_yandex_cloud_priv_validation_proto_rawDesc = []byte{
	0x0a, 0x22, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2f, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x70,
	0x72, 0x69, 0x76, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x11, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x63, 0x6c, 0x6f,
	0x75, 0x64, 0x2e, 0x70, 0x72, 0x69, 0x76, 0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70,
	0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x54, 0x0a, 0x0a, 0x4d, 0x61, 0x70,
	0x4b, 0x65, 0x79, 0x53, 0x70, 0x65, 0x63, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x18, 0x0a,
	0x07, 0x70, 0x61, 0x74, 0x74, 0x65, 0x72, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07,
	0x70, 0x61, 0x74, 0x74, 0x65, 0x72, 0x6e, 0x12, 0x16, 0x0a, 0x06, 0x6c, 0x65, 0x6e, 0x67, 0x74,
	0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x3a,
	0x40, 0x0a, 0x0b, 0x65, 0x78, 0x61, 0x63, 0x74, 0x6c, 0x79, 0x5f, 0x6f, 0x6e, 0x65, 0x12, 0x1d,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x4f, 0x6e, 0x65, 0x6f, 0x66, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xb0, 0x90,
	0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x65, 0x78, 0x61, 0x63, 0x74, 0x6c, 0x79, 0x4f, 0x6e,
	0x65, 0x3a, 0x3b, 0x0a, 0x08, 0x72, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x64, 0x12, 0x1d, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x95, 0x91, 0x06,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x72, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x64, 0x3a, 0x39,
	0x0a, 0x07, 0x70, 0x61, 0x74, 0x74, 0x65, 0x72, 0x6e, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c,
	0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x96, 0x91, 0x06, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x07, 0x70, 0x61, 0x74, 0x74, 0x65, 0x72, 0x6e, 0x3a, 0x35, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x18, 0x97, 0x91, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x3a, 0x33, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64,
	0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x98, 0x91, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x73, 0x69, 0x7a, 0x65, 0x3a, 0x37, 0x0a, 0x06, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x12,
	0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x99,
	0x91, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x3a, 0x37,
	0x0a, 0x06, 0x75, 0x6e, 0x69, 0x71, 0x75, 0x65, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64,
	0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x9a, 0x91, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x06, 0x75, 0x6e, 0x69, 0x71, 0x75, 0x65, 0x3a, 0x57, 0x0a, 0x07, 0x6d, 0x61, 0x70, 0x5f, 0x6b,
	0x65, 0x79, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x18, 0x9e, 0x91, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x79, 0x61, 0x6e, 0x64,
	0x65, 0x78, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x69, 0x76, 0x2e, 0x4d, 0x61,
	0x70, 0x4b, 0x65, 0x79, 0x53, 0x70, 0x65, 0x63, 0x52, 0x06, 0x6d, 0x61, 0x70, 0x4b, 0x65, 0x79,
	0x3a, 0x35, 0x0a, 0x05, 0x62, 0x79, 0x74, 0x65, 0x73, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c,
	0x64, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x9f, 0x91, 0x06, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x62, 0x79, 0x74, 0x65, 0x73, 0x42, 0x46, 0x5a, 0x44, 0x61, 0x2e, 0x79, 0x61, 0x6e,
	0x64, 0x65, 0x78, 0x2d, 0x74, 0x65, 0x61, 0x6d, 0x2e, 0x72, 0x75, 0x2f, 0x63, 0x6c, 0x6f, 0x75,
	0x64, 0x2f, 0x62, 0x69, 0x74, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x2f, 0x70, 0x72, 0x69, 0x76,
	0x61, 0x74, 0x65, 0x2d, 0x61, 0x70, 0x69, 0x2f, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2f, 0x63,
	0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x70, 0x72, 0x69, 0x76, 0x3b, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_yandex_cloud_priv_validation_proto_rawDescOnce sync.Once
	file_yandex_cloud_priv_validation_proto_rawDescData = file_yandex_cloud_priv_validation_proto_rawDesc
)

func file_yandex_cloud_priv_validation_proto_rawDescGZIP() []byte {
	file_yandex_cloud_priv_validation_proto_rawDescOnce.Do(func() {
		file_yandex_cloud_priv_validation_proto_rawDescData = protoimpl.X.CompressGZIP(file_yandex_cloud_priv_validation_proto_rawDescData)
	})
	return file_yandex_cloud_priv_validation_proto_rawDescData
}

var file_yandex_cloud_priv_validation_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_yandex_cloud_priv_validation_proto_goTypes = []interface{}{
	(*MapKeySpec)(nil),                // 0: yandex.cloud.priv.MapKeySpec
	(*descriptorpb.OneofOptions)(nil), // 1: google.protobuf.OneofOptions
	(*descriptorpb.FieldOptions)(nil), // 2: google.protobuf.FieldOptions
}
var file_yandex_cloud_priv_validation_proto_depIdxs = []int32{
	1,  // 0: yandex.cloud.priv.exactly_one:extendee -> google.protobuf.OneofOptions
	2,  // 1: yandex.cloud.priv.required:extendee -> google.protobuf.FieldOptions
	2,  // 2: yandex.cloud.priv.pattern:extendee -> google.protobuf.FieldOptions
	2,  // 3: yandex.cloud.priv.value:extendee -> google.protobuf.FieldOptions
	2,  // 4: yandex.cloud.priv.size:extendee -> google.protobuf.FieldOptions
	2,  // 5: yandex.cloud.priv.length:extendee -> google.protobuf.FieldOptions
	2,  // 6: yandex.cloud.priv.unique:extendee -> google.protobuf.FieldOptions
	2,  // 7: yandex.cloud.priv.map_key:extendee -> google.protobuf.FieldOptions
	2,  // 8: yandex.cloud.priv.bytes:extendee -> google.protobuf.FieldOptions
	0,  // 9: yandex.cloud.priv.map_key:type_name -> yandex.cloud.priv.MapKeySpec
	10, // [10:10] is the sub-list for method output_type
	10, // [10:10] is the sub-list for method input_type
	9,  // [9:10] is the sub-list for extension type_name
	0,  // [0:9] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
}

func init() { file_yandex_cloud_priv_validation_proto_init() }
func file_yandex_cloud_priv_validation_proto_init() {
	if File_yandex_cloud_priv_validation_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_yandex_cloud_priv_validation_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MapKeySpec); i {
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
			RawDescriptor: file_yandex_cloud_priv_validation_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 9,
			NumServices:   0,
		},
		GoTypes:           file_yandex_cloud_priv_validation_proto_goTypes,
		DependencyIndexes: file_yandex_cloud_priv_validation_proto_depIdxs,
		MessageInfos:      file_yandex_cloud_priv_validation_proto_msgTypes,
		ExtensionInfos:    file_yandex_cloud_priv_validation_proto_extTypes,
	}.Build()
	File_yandex_cloud_priv_validation_proto = out.File
	file_yandex_cloud_priv_validation_proto_rawDesc = nil
	file_yandex_cloud_priv_validation_proto_goTypes = nil
	file_yandex_cloud_priv_validation_proto_depIdxs = nil
}
