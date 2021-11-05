package telemetryclientfuzz // rename if needed

import (
	"sync"

	"storj.io/common/encryption"
	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/piecestore"
	"storj.io/uplink/private/storage/streams"
	// fill in manually if needed, or run 'goimports'
)

func Fuzz_Access_OverrideEncryptionKey(access *uplink.Access, bucket string, prefix string, encryptionKey *uplink.EncryptionKey) {
	if access == nil {
		return
	}
	if encryptionKey == nil {
		return
	}
	access.OverrideEncryptionKey(bucket, prefix, encryptionKey)
}

func Fuzz_Access_SatelliteAddress(access *uplink.Access) {
	if access == nil {
		return
	}
	access.SatelliteAddress()
}

func Fuzz_Access_Serialize(access *uplink.Access) {
	if access == nil {
		return
	}
	access.Serialize()
}

func Fuzz_Access_Share(access *uplink.Access, permission uplink.Permission, prefixes []uplink.SharePrefix) {
	if access == nil {
		return
	}
	access.Share(permission, prefixes...)
}

func Fuzz_BucketIterator_Err(buckets *uplink.BucketIterator) {
	if buckets == nil {
		return
	}
	buckets.Err()
}

func Fuzz_BucketIterator_Item(buckets *uplink.BucketIterator) {
	if buckets == nil {
		return
	}
	buckets.Item()
}

func Fuzz_BucketIterator_Next(buckets *uplink.BucketIterator) {
	if buckets == nil {
		return
	}
	buckets.Next()
}

func Fuzz_Download_Close(download *uplink.Download) {
	if download == nil {
		return
	}
	download.Close()
}

func Fuzz_Download_Info(download *uplink.Download) {
	if download == nil {
		return
	}
	download.Info()
}

func Fuzz_Download_Read(download *uplink.Download, p []byte) {
	if download == nil {
		return
	}
	download.Read(p)
}

func Fuzz_ObjectIterator_Err(objects *uplink.ObjectIterator) {
	if objects == nil {
		return
	}
	objects.Err()
}

func Fuzz_ObjectIterator_Item(objects *uplink.ObjectIterator) {
	if objects == nil {
		return
	}
	objects.Item()
}

func Fuzz_ObjectIterator_Next(objects *uplink.ObjectIterator) {
	if objects == nil {
		return
	}
	objects.Next()
}

func Fuzz_PartIterator_Err(parts *uplink.PartIterator) {
	if parts == nil {
		return
	}
	parts.Err()
}

func Fuzz_PartIterator_Item(parts *uplink.PartIterator) {
	if parts == nil {
		return
	}
	parts.Item()
}

func Fuzz_PartIterator_Next(parts *uplink.PartIterator) {
	if parts == nil {
		return
	}
	parts.Next()
}

func Fuzz_PartUpload_Abort(upload *uplink.PartUpload) {
	if upload == nil {
		return
	}
	upload.Abort()
}

func Fuzz_PartUpload_Commit(upload *uplink.PartUpload) {
	if upload == nil {
		return
	}
	upload.Commit()
}

func Fuzz_PartUpload_Info(upload *uplink.PartUpload) {
	if upload == nil {
		return
	}
	upload.Info()
}

func Fuzz_PartUpload_SetETag(upload *uplink.PartUpload, etag []byte) {
	if upload == nil {
		return
	}
	upload.SetETag(etag)
}

func Fuzz_PartUpload_Write(upload *uplink.PartUpload, p []byte) {
	if upload == nil {
		return
	}
	upload.Write(p)
}

// skipping Fuzz_Project_AbortUpload because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_BeginUpload because parameters include interfaces or funcs: context.Context

func Fuzz_Project_Close(project *uplink.Project) {
	if project == nil {
		return
	}
	project.Close()
}

// skipping Fuzz_Project_CommitUpload because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_CreateBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_DeleteBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_DeleteBucketWithObjects because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_DeleteObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_DownloadObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_EnsureBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_ListBuckets because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_ListObjects because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_ListUploadParts because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_ListUploads because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_RevokeAccess because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_StatBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_StatObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_UpdateObjectMetadata because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_UploadObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Project_UploadPart because parameters include interfaces or funcs: context.Context

func Fuzz_Upload_Abort(upload *uplink.Upload) {
	if upload == nil {
		return
	}
	upload.Abort()
}

func Fuzz_Upload_Commit(upload *uplink.Upload) {
	if upload == nil {
		return
	}
	upload.Commit()
}

func Fuzz_Upload_Info(upload *uplink.Upload) {
	if upload == nil {
		return
	}
	upload.Info()
}

// skipping Fuzz_Upload_SetCustomMetadata because parameters include interfaces or funcs: context.Context

func Fuzz_Upload_Write(upload *uplink.Upload, p []byte) {
	if upload == nil {
		return
	}
	upload.Write(p)
}

func Fuzz_UploadIterator_Err(uploads *uplink.UploadIterator) {
	if uploads == nil {
		return
	}
	uploads.Err()
}

func Fuzz_UploadIterator_Item(uploads *uplink.UploadIterator) {
	if uploads == nil {
		return
	}
	uploads.Item()
}

func Fuzz_UploadIterator_Next(uploads *uplink.UploadIterator) {
	if uploads == nil {
		return
	}
	uploads.Next()
}

func Fuzz_EncodedRanger_OutputSize(er *eestream.EncodedRanger) {
	if er == nil {
		return
	}
	er.OutputSize()
}

// skipping Fuzz_EncodedRanger_Range because parameters include interfaces or funcs: context.Context

func Fuzz_PieceBuffer_Close(buf []byte, shareSize int, newDataCond *sync.Cond) {
	if newDataCond == nil {
		return
	}
	b := eestream.NewPieceBuffer(buf, shareSize, newDataCond)
	b.Close()
}

func Fuzz_PieceBuffer_HasShare(buf []byte, shareSize int, newDataCond *sync.Cond, num int64) {
	if newDataCond == nil {
		return
	}
	b := eestream.NewPieceBuffer(buf, shareSize, newDataCond)
	b.HasShare(num)
}

func Fuzz_PieceBuffer_Read(buf []byte, shareSize int, newDataCond *sync.Cond, p []byte) {
	if newDataCond == nil {
		return
	}
	b := eestream.NewPieceBuffer(buf, shareSize, newDataCond)
	b.Read(p)
}

func Fuzz_PieceBuffer_ReadShare(buf []byte, shareSize int, newDataCond *sync.Cond, num int64, p []byte) {
	if newDataCond == nil {
		return
	}
	b := eestream.NewPieceBuffer(buf, shareSize, newDataCond)
	b.ReadShare(num, p)
}

// skipping Fuzz_PieceBuffer_SetError because parameters include interfaces or funcs: error

func Fuzz_PieceBuffer_Skip(buf []byte, shareSize int, newDataCond *sync.Cond, n int) {
	if newDataCond == nil {
		return
	}
	b := eestream.NewPieceBuffer(buf, shareSize, newDataCond)
	b.Skip(n)
}

func Fuzz_PieceBuffer_Write(buf []byte, shareSize int, newDataCond *sync.Cond, p []byte) {
	if newDataCond == nil {
		return
	}
	b := eestream.NewPieceBuffer(buf, shareSize, newDataCond)
	b.Write(p)
}

func Fuzz_RedundancyStrategy_OptimalThreshold(rs *eestream.RedundancyStrategy) {
	if rs == nil {
		return
	}
	rs.OptimalThreshold()
}

func Fuzz_RedundancyStrategy_RepairThreshold(rs *eestream.RedundancyStrategy) {
	if rs == nil {
		return
	}
	rs.RepairThreshold()
}

// skipping Fuzz_StripeReader_Close because parameters include interfaces or funcs: storj.io/uplink/private/eestream.ErasureScheme

// skipping Fuzz_StripeReader_ReadStripe because parameters include interfaces or funcs: storj.io/uplink/private/eestream.ErasureScheme

// skipping Fuzz_HashReader_CurrentETag because parameters include interfaces or funcs: io.Reader

// skipping Fuzz_HashReader_Read because parameters include interfaces or funcs: io.Reader

func Fuzz_BatchResponse_BeginDeleteObject(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.BeginDeleteObject()
}

func Fuzz_BatchResponse_BeginObject(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.BeginObject()
}

func Fuzz_BatchResponse_BeginSegment(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.BeginSegment()
}

func Fuzz_BatchResponse_CreateBucket(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.CreateBucket()
}

func Fuzz_BatchResponse_DownloadSegment(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.DownloadSegment()
}

func Fuzz_BatchResponse_GetBucket(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.GetBucket()
}

func Fuzz_BatchResponse_GetObject(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.GetObject()
}

func Fuzz_BatchResponse_ListBuckets(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.ListBuckets()
}

func Fuzz_BatchResponse_ListObjects(resp *metaclient.BatchResponse) {
	if resp == nil {
		return
	}
	resp.ListObjects()
}

func Fuzz_BeginDeleteObjectParams_BatchItem(params *metaclient.BeginDeleteObjectParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_BeginObjectParams_BatchItem(params *metaclient.BeginObjectParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_BeginSegmentParams_BatchItem(params *metaclient.BeginSegmentParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

// skipping Fuzz_Client_Batch because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_BeginDeleteObject because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_BeginObject because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_BeginSegment because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_Close because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_CommitObject because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_CommitSegment because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_CreateBucket because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_DeleteBucket because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_DownloadObject because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_DownloadSegment because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_DownloadSegmentWithRS because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_GetBucket because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_GetObject because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_GetObjectIPs because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_GetProjectInfo because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_ListBuckets because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_ListObjects because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_ListPendingObjectStreams because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_ListSegments because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_MakeInlineSegment because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_RevokeAPIKey because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_SetRawAPIKey because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

// skipping Fuzz_Client_UpdateObjectMetadata because parameters include interfaces or funcs: storj.io/common/pb.DRPCMetainfoClient

func Fuzz_CommitObjectParams_BatchItem(params *metaclient.CommitObjectParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_CommitSegmentParams_BatchItem(params *metaclient.CommitSegmentParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_CreateBucketParams_BatchItem(params *metaclient.CreateBucketParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_DB_Close(metainfo *metaclient.Client, encStore *encryption.Store) {
	if metainfo == nil {
		return
	}
	if encStore == nil {
		return
	}
	db := metaclient.New(metainfo, encStore)
	db.Close()
}

// skipping Fuzz_DB_CreateBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_CreateObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_DeleteBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_DeleteObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_DownloadObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_GetBucket because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_GetObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_GetObjectIPs because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_ListBuckets because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_ListObjects because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_ListPendingObjectStreams because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_ListPendingObjects because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_ModifyObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_ModifyPendingObject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_DB_UpdateObjectMetadata because parameters include interfaces or funcs: context.Context

func Fuzz_DeleteBucketParams_BatchItem(params *metaclient.DeleteBucketParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_DownloadObjectParams_BatchItem(params *metaclient.DownloadObjectParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_DownloadSegmentParams_BatchItem(params *metaclient.DownloadSegmentParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_ExponentialBackoff_Maxed(e *metaclient.ExponentialBackoff) {
	if e == nil {
		return
	}
	e.Maxed()
}

func Fuzz_ExponentialBackoff_Wait(e *metaclient.ExponentialBackoff) {
	if e == nil {
		return
	}
	e.Wait()
}

func Fuzz_GetBucketParams_BatchItem(params *metaclient.GetBucketParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_GetObjectParams_BatchItem(params *metaclient.GetObjectParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_ListBucketsParams_BatchItem(params *metaclient.ListBucketsParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_ListObjectsParams_BatchItem(params *metaclient.ListObjectsParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_ListPendingObjectStreamsParams_BatchItem(params *metaclient.ListPendingObjectStreamsParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_ListSegmentsParams_BatchItem(params *metaclient.ListSegmentsParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

func Fuzz_MakeInlineSegmentParams_BatchItem(params *metaclient.MakeInlineSegmentParams) {
	if params == nil {
		return
	}
	params.BatchItem()
}

// skipping Fuzz_MutableObject_CreateDynamicStream because parameters include interfaces or funcs: context.Context

// skipping Fuzz_MutableObject_CreateStream because parameters include interfaces or funcs: context.Context

func Fuzz_MutableObject_Info(object *metaclient.MutableObject) {
	if object == nil {
		return
	}
	object.Info()
}

func Fuzz_MutableStream_BucketName(stream *metaclient.MutableStream) {
	if stream == nil {
		return
	}
	stream.BucketName()
}

func Fuzz_MutableStream_Expires(stream *metaclient.MutableStream) {
	if stream == nil {
		return
	}
	stream.Expires()
}

func Fuzz_MutableStream_Info(stream *metaclient.MutableStream) {
	if stream == nil {
		return
	}
	stream.Info()
}

func Fuzz_MutableStream_Metadata(stream *metaclient.MutableStream) {
	if stream == nil {
		return
	}
	stream.Metadata()
}

func Fuzz_MutableStream_Path(stream *metaclient.MutableStream) {
	if stream == nil {
		return
	}
	stream.Path()
}

func Fuzz_Client_Close(client *piecestore.Client) {
	if client == nil {
		return
	}
	client.Close()
}

// skipping Fuzz_Client_Download because parameters include interfaces or funcs: context.Context

func Fuzz_Client_GetPeerIdentity(client *piecestore.Client) {
	if client == nil {
		return
	}
	client.GetPeerIdentity()
}

// skipping Fuzz_Client_Retain because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Client_UploadReader because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Client_VerifyPieceHash because parameters include interfaces or funcs: context.Context

func Fuzz_Download_Close(client *piecestore.Download) {
	if client == nil {
		return
	}
	client.Close()
}

func Fuzz_Download_GetHashAndLimit(client *piecestore.Download) {
	if client == nil {
		return
	}
	client.GetHashAndLimit()
}

func Fuzz_Download_Read(client *piecestore.Download, data []byte) {
	if client == nil {
		return
	}
	client.Read(data)
}

func Fuzz_LockingDownload_Close(download *piecestore.LockingDownload) {
	if download == nil {
		return
	}
	download.Close()
}

func Fuzz_LockingDownload_GetHashAndLimit(download *piecestore.LockingDownload) {
	if download == nil {
		return
	}
	download.GetHashAndLimit()
}

func Fuzz_LockingDownload_Read(download *piecestore.LockingDownload, p []byte) {
	if download == nil {
		return
	}
	download.Read(p)
}

func Fuzz_ReadBuffer_Empty(buffer *piecestore.ReadBuffer) {
	if buffer == nil {
		return
	}
	buffer.Empty()
}

func Fuzz_ReadBuffer_Error(buffer *piecestore.ReadBuffer) {
	if buffer == nil {
		return
	}
	buffer.Error()
}

func Fuzz_ReadBuffer_Errored(buffer *piecestore.ReadBuffer) {
	if buffer == nil {
		return
	}
	buffer.Errored()
}

func Fuzz_ReadBuffer_Fill(buffer *piecestore.ReadBuffer, data []byte) {
	if buffer == nil {
		return
	}
	buffer.Fill(data)
}

// skipping Fuzz_ReadBuffer_IncludeError because parameters include interfaces or funcs: error

func Fuzz_ReadBuffer_Read(buffer *piecestore.ReadBuffer, data []byte) {
	if buffer == nil {
		return
	}
	buffer.Read(data)
}

// skipping Fuzz_EOFReader_HasError because parameters include interfaces or funcs: io.Reader

// skipping Fuzz_EOFReader_IsEOF because parameters include interfaces or funcs: io.Reader

// skipping Fuzz_EOFReader_Read because parameters include interfaces or funcs: io.Reader

// skipping Fuzz_PeekThresholdReader_IsLargerThan because parameters include interfaces or funcs: io.Reader

// skipping Fuzz_PeekThresholdReader_Read because parameters include interfaces or funcs: io.Reader

func Fuzz_SizedReader_Read(r *streams.SizedReader, p []byte) {
	if r == nil {
		return
	}
	r.Read(p)
}

func Fuzz_SizedReader_Size(r *streams.SizedReader) {
	if r == nil {
		return
	}
	r.Size()
}

func Fuzz_Store_Close(s *streams.Store) {
	if s == nil {
		return
	}
	s.Close()
}

// skipping Fuzz_Store_Get because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Store_Put because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Store_PutPart because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Store_Ranger because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Download_Close because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Download_Read because parameters include interfaces or funcs: context.Context

// skipping Fuzz_PartUpload_Abort because parameters include interfaces or funcs: context.Context

// skipping Fuzz_PartUpload_Close because parameters include interfaces or funcs: context.Context

// skipping Fuzz_PartUpload_Part because parameters include interfaces or funcs: context.Context

// skipping Fuzz_PartUpload_Write because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Upload_Abort because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Upload_Close because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Upload_Meta because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Upload_Write because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Config_OpenProject because parameters include interfaces or funcs: context.Context

// skipping Fuzz_Config_RequestAccessWithPassphrase because parameters include interfaces or funcs: context.Context

func Fuzz_CustomMetadata_Clone(meta uplink.CustomMetadata) {
	meta.Clone()
}

func Fuzz_CustomMetadata_Verify(meta uplink.CustomMetadata) {
	meta.Verify()
}

func Fuzz_CreateObject_Object(create metaclient.CreateObject, bucket storj.Bucket, path string) {
	create.Object(bucket, path)
}

func Fuzz_StreamRange_Normalize(streamRange metaclient.StreamRange, plainSize int64) {
	streamRange.Normalize(plainSize)
}

func Fuzz_DeriveEncryptionKey(passphrase string, salt []byte) {
	uplink.DeriveEncryptionKey(passphrase, salt)
}

// skipping Fuzz_OpenProject because parameters include interfaces or funcs: context.Context

func Fuzz_ParseAccess(access string) {
	uplink.ParseAccess(access)
}
