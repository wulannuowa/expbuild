package cas

import (
	"context"

	"github.com/expbuild/expbuild/util/log"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
)

type CASStore interface {
	HasBlob(digest *pb.Digest) bool
	GetBlob(digest *pb.Digest) ([]byte, error)
}

type CASServer struct {
	pb.UnimplementedContentAddressableStorageServer
	Store CASStore
}

func (s *CASServer) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	log.Debugf("Received find missing request: %v", req.GetBlobDigests())
	missing_digests := []*pb.Digest{}
	for _, digest := range req.GetBlobDigests() {
		if !s.Store.HasBlob(digest) {
			missing_digests = append(missing_digests, digest)
		}
	}
	response := pb.FindMissingBlobsResponse{
		MissingBlobDigests: missing_digests,
	}
	return &response, nil
}
