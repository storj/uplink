// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: cursed.proto

package cursed

import (
	fmt "fmt"
	math "math"
	time "time"

	proto "github.com/gogo/protobuf/proto"

	pb "storj.io/common/pb"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf
var _ = time.Kitchen

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type StreamID struct {
	Bucket               []byte                   `protobuf:"bytes,1,opt,name=bucket,proto3" json:"bucket,omitempty"`
	EncryptedObjectKey   []byte                   `protobuf:"bytes,2,opt,name=encrypted_object_key,json=encryptedObjectKey,proto3" json:"encrypted_object_key,omitempty"`
	Version              int64                    `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	EncryptionParameters *pb.EncryptionParameters `protobuf:"bytes,12,opt,name=encryption_parameters,json=encryptionParameters,proto3" json:"encryption_parameters,omitempty"`
	CreationDate         time.Time                `protobuf:"bytes,5,opt,name=creation_date,json=creationDate,proto3,stdtime" json:"creation_date"`
	ExpirationDate       time.Time                `protobuf:"bytes,6,opt,name=expiration_date,json=expirationDate,proto3,stdtime" json:"expiration_date"`
	MultipartObject      bool                     `protobuf:"varint,11,opt,name=multipart_object,json=multipartObject,proto3" json:"multipart_object,omitempty"`
	SatelliteSignature   []byte                   `protobuf:"bytes,9,opt,name=satellite_signature,json=satelliteSignature,proto3" json:"satellite_signature,omitempty"`
	StreamId             []byte                   `protobuf:"bytes,10,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	Placement            int32                    `protobuf:"varint,13,opt,name=placement,proto3" json:"placement,omitempty"`
	Versioned            bool                     `protobuf:"varint,15,opt,name=versioned,proto3" json:"versioned,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                 `json:"-"`
	XXX_unrecognized     []byte                   `json:"-"`
	XXX_sizecache        int32                    `json:"-"`
}

func (m *StreamID) Reset()         { *m = StreamID{} }
func (m *StreamID) String() string { return proto.CompactTextString(m) }
func (*StreamID) ProtoMessage()    {}
func (*StreamID) Descriptor() ([]byte, []int) {
	return fileDescriptor_529e2a3eab83a951, []int{0}
}
func (m *StreamID) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StreamID.Unmarshal(m, b)
}
func (m *StreamID) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StreamID.Marshal(b, m, deterministic)
}
func (m *StreamID) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamID.Merge(m, src)
}
func (m *StreamID) XXX_Size() int {
	return xxx_messageInfo_StreamID.Size(m)
}
func (m *StreamID) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamID.DiscardUnknown(m)
}

var xxx_messageInfo_StreamID proto.InternalMessageInfo

func (m *StreamID) GetBucket() []byte {
	if m != nil {
		return m.Bucket
	}
	return nil
}

func (m *StreamID) GetEncryptedObjectKey() []byte {
	if m != nil {
		return m.EncryptedObjectKey
	}
	return nil
}

func (m *StreamID) GetVersion() int64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *StreamID) GetEncryptionParameters() *pb.EncryptionParameters {
	if m != nil {
		return m.EncryptionParameters
	}
	return nil
}

func (m *StreamID) GetCreationDate() time.Time {
	if m != nil {
		return m.CreationDate
	}
	return time.Time{}
}

func (m *StreamID) GetExpirationDate() time.Time {
	if m != nil {
		return m.ExpirationDate
	}
	return time.Time{}
}

func (m *StreamID) GetMultipartObject() bool {
	if m != nil {
		return m.MultipartObject
	}
	return false
}

func (m *StreamID) GetSatelliteSignature() []byte {
	if m != nil {
		return m.SatelliteSignature
	}
	return nil
}

func (m *StreamID) GetStreamId() []byte {
	if m != nil {
		return m.StreamId
	}
	return nil
}

func (m *StreamID) GetPlacement() int32 {
	if m != nil {
		return m.Placement
	}
	return 0
}

func (m *StreamID) GetVersioned() bool {
	if m != nil {
		return m.Versioned
	}
	return false
}

type SegmentID struct {
	StreamId             *StreamID                 `protobuf:"bytes,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	PartNumber           int32                     `protobuf:"varint,2,opt,name=part_number,json=partNumber,proto3" json:"part_number,omitempty"`
	Index                int32                     `protobuf:"varint,3,opt,name=index,proto3" json:"index,omitempty"`
	RootPieceId          PieceID                   `protobuf:"bytes,5,opt,name=root_piece_id,json=rootPieceId,proto3,customtype=PieceID" json:"root_piece_id"`
	OriginalOrderLimits  []*pb.AddressedOrderLimit `protobuf:"bytes,6,rep,name=original_order_limits,json=originalOrderLimits,proto3" json:"original_order_limits,omitempty"`
	CreationDate         time.Time                 `protobuf:"bytes,7,opt,name=creation_date,json=creationDate,proto3,stdtime" json:"creation_date"`
	RedundancyScheme     *pb.RedundancyScheme      `protobuf:"bytes,9,opt,name=redundancy_scheme,json=redundancyScheme,proto3" json:"redundancy_scheme,omitempty"`
	SatelliteSignature   []byte                    `protobuf:"bytes,8,opt,name=satellite_signature,json=satelliteSignature,proto3" json:"satellite_signature,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                  `json:"-"`
	XXX_unrecognized     []byte                    `json:"-"`
	XXX_sizecache        int32                     `json:"-"`
}

func (m *SegmentID) Reset()         { *m = SegmentID{} }
func (m *SegmentID) String() string { return proto.CompactTextString(m) }
func (*SegmentID) ProtoMessage()    {}
func (*SegmentID) Descriptor() ([]byte, []int) {
	return fileDescriptor_529e2a3eab83a951, []int{1}
}
func (m *SegmentID) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SegmentID.Unmarshal(m, b)
}
func (m *SegmentID) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SegmentID.Marshal(b, m, deterministic)
}
func (m *SegmentID) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SegmentID.Merge(m, src)
}
func (m *SegmentID) XXX_Size() int {
	return xxx_messageInfo_SegmentID.Size(m)
}
func (m *SegmentID) XXX_DiscardUnknown() {
	xxx_messageInfo_SegmentID.DiscardUnknown(m)
}

var xxx_messageInfo_SegmentID proto.InternalMessageInfo

func (m *SegmentID) GetStreamId() *StreamID {
	if m != nil {
		return m.StreamId
	}
	return nil
}

func (m *SegmentID) GetPartNumber() int32 {
	if m != nil {
		return m.PartNumber
	}
	return 0
}

func (m *SegmentID) GetIndex() int32 {
	if m != nil {
		return m.Index
	}
	return 0
}

func (m *SegmentID) GetOriginalOrderLimits() []*pb.AddressedOrderLimit {
	if m != nil {
		return m.OriginalOrderLimits
	}
	return nil
}

func (m *SegmentID) GetCreationDate() time.Time {
	if m != nil {
		return m.CreationDate
	}
	return time.Time{}
}

func (m *SegmentID) GetRedundancyScheme() *pb.RedundancyScheme {
	if m != nil {
		return m.RedundancyScheme
	}
	return nil
}

func (m *SegmentID) GetSatelliteSignature() []byte {
	if m != nil {
		return m.SatelliteSignature
	}
	return nil
}

func init() {
	proto.RegisterType((*StreamID)(nil), "uplink.cursed.StreamID")
	proto.RegisterType((*SegmentID)(nil), "uplink.cursed.SegmentID")
}

func init() { proto.RegisterFile("cursed.proto", fileDescriptor_529e2a3eab83a951) }

var fileDescriptor_529e2a3eab83a951 = []byte{
	// 612 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x53, 0xdd, 0x6e, 0xd3, 0x30,
	0x14, 0x26, 0x74, 0xed, 0x5a, 0xb7, 0x5d, 0x8b, 0xb7, 0x41, 0xb4, 0x81, 0x1a, 0xed, 0xaa, 0x13,
	0x52, 0x82, 0x36, 0x5e, 0x80, 0xa9, 0x48, 0x94, 0xbf, 0x8d, 0x14, 0x6e, 0xb8, 0x89, 0xdc, 0xf8,
	0x2c, 0x78, 0x4b, 0xec, 0xc8, 0x76, 0xa6, 0xf5, 0x2d, 0xb8, 0xe6, 0x81, 0x10, 0xcf, 0xc0, 0xc5,
	0x78, 0x15, 0x14, 0xbb, 0x49, 0x0b, 0xda, 0x2e, 0xe0, 0x2e, 0xe7, 0x3b, 0x9f, 0xbf, 0xf8, 0x7c,
	0xe7, 0x33, 0xea, 0xc5, 0x85, 0x54, 0x40, 0xfd, 0x5c, 0x0a, 0x2d, 0x70, 0xbf, 0xc8, 0x53, 0xc6,
	0x2f, 0x7d, 0x0b, 0xee, 0xa1, 0x44, 0x24, 0xc2, 0xb6, 0xf6, 0x46, 0x89, 0x10, 0x49, 0x0a, 0x81,
	0xa9, 0xe6, 0xc5, 0x79, 0xa0, 0x59, 0x06, 0x4a, 0x93, 0x2c, 0x5f, 0x12, 0x86, 0xc0, 0x63, 0xb9,
	0xc8, 0x35, 0x13, 0x7c, 0x89, 0x6c, 0x65, 0xa0, 0x09, 0xe3, 0xe7, 0x95, 0xc4, 0x20, 0x17, 0x8c,
	0x6b, 0x90, 0x74, 0x6e, 0x81, 0x83, 0x6f, 0x1b, 0xa8, 0x3d, 0xd3, 0x12, 0x48, 0x36, 0x9d, 0xe0,
	0x87, 0xa8, 0x35, 0x2f, 0xe2, 0x4b, 0xd0, 0xae, 0xe3, 0x39, 0xe3, 0x5e, 0xb8, 0xac, 0xf0, 0x33,
	0xb4, 0xb3, 0x54, 0x06, 0x1a, 0x89, 0xf9, 0x05, 0xc4, 0x3a, 0xba, 0x84, 0x85, 0x7b, 0xdf, 0xb0,
	0x70, 0xdd, 0x3b, 0x35, 0xad, 0x37, 0xb0, 0xc0, 0x2e, 0xda, 0xbc, 0x02, 0xa9, 0x98, 0xe0, 0x6e,
	0xc3, 0x73, 0xc6, 0x8d, 0xb0, 0x2a, 0xf1, 0x27, 0xb4, 0xbb, 0xba, 0x65, 0x94, 0x13, 0x49, 0x32,
	0xd0, 0x20, 0x95, 0xdb, 0xf3, 0x9c, 0x71, 0xf7, 0xc8, 0xf3, 0xd7, 0x66, 0x78, 0x59, 0x7f, 0x9e,
	0xd5, 0xbc, 0x70, 0x07, 0x6e, 0x41, 0xf1, 0x14, 0xf5, 0x63, 0x09, 0xc4, 0x88, 0x52, 0xa2, 0xc1,
	0x6d, 0x1a, 0xb9, 0x3d, 0xdf, 0x7a, 0xe6, 0x57, 0x9e, 0xf9, 0x1f, 0x2b, 0xcf, 0x4e, 0xda, 0x3f,
	0x6e, 0x46, 0xf7, 0xbe, 0xfe, 0x1a, 0x39, 0x61, 0xaf, 0x3a, 0x3a, 0x21, 0x1a, 0xf0, 0x3b, 0x34,
	0x80, 0xeb, 0x9c, 0xc9, 0x35, 0xb1, 0xd6, 0x3f, 0x88, 0x6d, 0xad, 0x0e, 0x1b, 0xb9, 0x43, 0x34,
	0xcc, 0x8a, 0x54, 0xb3, 0x9c, 0x48, 0xbd, 0x34, 0xcf, 0xed, 0x7a, 0xce, 0xb8, 0x1d, 0x0e, 0x6a,
	0xdc, 0x1a, 0x87, 0x03, 0xb4, 0xad, 0x88, 0x86, 0x34, 0x65, 0x1a, 0x22, 0xc5, 0x12, 0x4e, 0x74,
	0x21, 0xc1, 0xed, 0x58, 0x9b, 0xeb, 0xd6, 0xac, 0xea, 0xe0, 0x7d, 0xd4, 0x51, 0x66, 0x79, 0x11,
	0xa3, 0x2e, 0x32, 0xb4, 0xb6, 0x05, 0xa6, 0x14, 0x3f, 0x46, 0x9d, 0x3c, 0x25, 0x31, 0x64, 0xc0,
	0xb5, 0xdb, 0xf7, 0x9c, 0x71, 0x33, 0x5c, 0x01, 0x65, 0x77, 0xb9, 0x12, 0xa0, 0xee, 0xc0, 0xdc,
	0x67, 0x05, 0xbc, 0xde, 0x68, 0x6f, 0x0d, 0x07, 0x07, 0xdf, 0x1b, 0xa8, 0x33, 0x83, 0xa4, 0xe4,
	0x4f, 0x27, 0xf8, 0xf9, 0xfa, 0xcf, 0x1c, 0xe3, 0xc8, 0x23, 0xff, 0x8f, 0xb4, 0xfa, 0x55, 0x92,
	0xd6, 0x6e, 0x31, 0x42, 0x5d, 0x33, 0x39, 0x2f, 0xb2, 0x39, 0x48, 0x13, 0x99, 0x66, 0x88, 0x4a,
	0xe8, 0xbd, 0x41, 0xf0, 0x0e, 0x6a, 0x32, 0x4e, 0xe1, 0xda, 0x04, 0xa5, 0x19, 0xda, 0x02, 0x1f,
	0xa3, 0xbe, 0x14, 0x42, 0x47, 0x39, 0x83, 0x18, 0xca, 0x1f, 0x96, 0xfb, 0xec, 0x9d, 0x0c, 0x4a,
	0x9b, 0x7f, 0xde, 0x8c, 0x36, 0xcf, 0x4a, 0x7c, 0x3a, 0x09, 0xbb, 0x25, 0xcb, 0x16, 0x14, 0x7f,
	0x40, 0xbb, 0x42, 0xb2, 0x84, 0x71, 0x92, 0x46, 0x42, 0x52, 0x90, 0x51, 0xca, 0x32, 0xa6, 0x95,
	0xdb, 0xf2, 0x1a, 0xe3, 0xee, 0xd1, 0x13, 0xbf, 0x7e, 0x0d, 0x2f, 0x28, 0x95, 0xa0, 0x14, 0xd0,
	0xd3, 0x92, 0xf6, 0xb6, 0x64, 0x85, 0xdb, 0xd5, 0xd9, 0x15, 0x76, 0x4b, 0xae, 0x36, 0xff, 0x3b,
	0x57, 0xaf, 0xd0, 0x03, 0x09, 0xb4, 0xe0, 0x94, 0xf0, 0x78, 0x11, 0xa9, 0xf8, 0x0b, 0x64, 0x76,
	0xb7, 0xdd, 0xa3, 0x7d, 0x7f, 0xf5, 0x2e, 0xc3, 0x9a, 0x33, 0x33, 0x94, 0x70, 0x28, 0xff, 0x42,
	0xee, 0xca, 0x49, 0xfb, 0xae, 0x9c, 0x9c, 0x3c, 0xfd, 0x7c, 0xa8, 0xb4, 0x90, 0x17, 0x3e, 0x13,
	0x81, 0xdd, 0x58, 0x90, 0x4b, 0x76, 0x45, 0x34, 0x04, 0xa5, 0x25, 0x71, 0xca, 0x80, 0xeb, 0xc0,
	0x2e, 0x71, 0xde, 0x32, 0x33, 0x1d, 0xff, 0x0e, 0x00, 0x00, 0xff, 0xff, 0xeb, 0xda, 0xc8, 0x8a,
	0x98, 0x04, 0x00, 0x00,
}
