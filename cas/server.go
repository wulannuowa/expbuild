package cas

import (
	"context"

	"github.com/expbuild/expbuild/util/log"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
)

type CASStore interface {
	HasBlob(ctx context.Context, digest *pb.Digest) bool
	GetBlob(ctx context.Context, digest *pb.Digest) ([]byte, error)
	FindMissingBlobs(ctx context.Context, digest []*pb.Digest) ([]*pb.Digest, error)
}

type CASServer struct {
	pb.UnimplementedContentAddressableStorageServer
	Store CASStore
}

func (s *CASServer) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	log.Debugf("Received find missing request: %v", req.GetBlobDigests())
	missing_digests := []*pb.Digest{}
	for _, digest := range req.GetBlobDigests() {
		if !s.Store.HasBlob(ctx, digest) {
			missing_digests = append(missing_digests, digest)
		}
	}
	response := pb.FindMissingBlobsResponse{
		MissingBlobDigests: missing_digests,
	}
	return &response, nil
}

func (s *CASServer) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	return nil, nil
}
func (s *CASServer) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	return nil, nil
}
func (s *CASServer) GetTree(req *pb.GetTreeRequest, stream pb.ContentAddressableStorage_GetTreeServer) error {
	return nil
}
