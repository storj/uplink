package stub

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"storj.io/common/identity"
	"storj.io/common/identity/testidentity"
	"storj.io/common/storj"
	"storj.io/drpc"
)

type stubNodes []*nodeStub

func NewStubNodes(size int) (stubNodes, error) {
	result := stubNodes{}
	for i := 0; i < size; i++ {
		node, err := NewNodeStub(int8(i))
		if err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	return result, nil
}

func (n stubNodes) GetByAddress(address string) (*nodeStub, error) {
	for _, node := range n {
		if node.Address == address {
			return node, nil
		}
	}
	return nil, fmt.Errorf("no such node %s", address)
}

type nodeStub struct {
	Address  string
	Identity *identity.FullIdentity
	Index    int8
}

func NewNodeStub(index int8) (*nodeStub, error) {
	otherIdentity, err := testidentity.PregeneratedIdentity(int(index), storj.LatestIDVersion())
	if err != nil {
		return nil, err
	}
	return &nodeStub{
		Address:  fmt.Sprintf("10.10.10.%d:1234", index),
		Identity: otherIdentity,
		Index:    index,
	}, nil
}

func (n *nodeStub) CreateUploadConnection() (drpc.Conn, *tls.ConnectionState, error) {
	return NewPieceStoreUploadStub(n), &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			n.Identity.Leaf,
			n.Identity.CA,
		},
	}, nil
}

func (n *nodeStub) CreateDownloadConnection(pieceTemplate []byte) (drpc.Conn, *tls.ConnectionState, error) {
	stub, err := NewPieceStoreDownloadStub(n, pieceTemplate)
	return stub, &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			n.Identity.Leaf,
			n.Identity.CA,
		},
	}, err
}
