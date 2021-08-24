// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient

import (
	"context"

	"storj.io/common/pb"
	"storj.io/common/storj"
)

// BeginMoveObjectParams parameters for BeginMoveObject method.
type BeginMoveObjectParams struct {
	Bucket             []byte
	EncryptedObjectKey []byte
}

// EncryptedKeyAndNonce holds single segment encrypted key.
type EncryptedKeyAndNonce struct {
	Position          SegmentPosition
	EncryptedKeyNonce storj.Nonce
	EncryptedKey      []byte
}

// BeginMoveObjectResponse response for BeginMoveObjectResponse request.
type BeginMoveObjectResponse struct {
	StreamID                  storj.StreamID
	EncryptedMetadataKeyNonce storj.Nonce
	EncryptedMetadataKey      []byte
	Keys                      []EncryptedKeyAndNonce
}

func (params *BeginMoveObjectParams) toRequest(header *pb.RequestHeader) *pb.ObjectBeginMoveRequest {
	return &pb.ObjectBeginMoveRequest{
		Header:             header,
		Bucket:             params.Bucket,
		EncryptedObjectKey: params.EncryptedObjectKey,
	}
}

// BatchItem returns single item for batch request.
func (params *BeginMoveObjectParams) BatchItem() *pb.BatchRequestItem {
	return &pb.BatchRequestItem{
		Request: &pb.BatchRequestItem_ObjectBeginMove{
			ObjectBeginMove: params.toRequest(nil),
		},
	}
}

func newBeginMoveObjectResponse(response *pb.ObjectBeginMoveResponse) BeginMoveObjectResponse {
	keys := make([]EncryptedKeyAndNonce, len(response.SegmentKeys))
	for i, key := range response.SegmentKeys {
		keys[i] = EncryptedKeyAndNonce{
			EncryptedKeyNonce: key.EncryptedKeyNonce,
			EncryptedKey:      key.EncryptedKey,
		}
		if key.Position != nil {
			keys[i].Position = storj.SegmentPosition{
				PartNumber: key.Position.PartNumber,
				Index:      key.Position.Index,
			}
		}
	}

	return BeginMoveObjectResponse{
		StreamID:                  response.StreamId,
		EncryptedMetadataKeyNonce: response.EncryptedMetadataKeyNonce,
		EncryptedMetadataKey:      response.EncryptedMetadataKey,
		Keys:                      keys,
	}
}

// BeginMoveObject begins process of moving object from one key to another.
func (client *Client) BeginMoveObject(ctx context.Context, params BeginMoveObjectParams) (_ BeginMoveObjectResponse, err error) {
	defer mon.Task()(&ctx)(&err)

	var response *pb.ObjectBeginMoveResponse
	err = WithRetry(ctx, func(ctx context.Context) error {
		response, err = client.client.BeginMoveObject(ctx, params.toRequest(client.header()))
		return err
	})
	if err != nil {
		return BeginMoveObjectResponse{}, Error.Wrap(err)
	}

	return newBeginMoveObjectResponse(response), nil
}
