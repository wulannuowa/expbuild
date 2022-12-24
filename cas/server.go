package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	pbbs "github.com/expbuild/expbuild/proto/gen/bytestream"
	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/util/log"
	code "google.golang.org/genproto/googleapis/rpc/code"
	status "google.golang.org/genproto/googleapis/rpc/status"
)

type CASStore interface {
	HasBlob(ctx context.Context, digest *pb.Digest) bool
	GetBlob(ctx context.Context, digest *pb.Digest) ([]byte, error)
	PutBlob(ctx context.Context, digest *pb.Digest, data []byte) error
	FindMissingBlobs(ctx context.Context, digest []*pb.Digest) ([]*pb.Digest, error)
}

type CASServer struct {
	pb.UnimplementedContentAddressableStorageServer
	pbbs.UnimplementedByteStreamServer
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
	log.Debugf("Recived BatchUpdateBlobs")
	response := pb.BatchUpdateBlobsResponse{}
	for _, r := range req.Requests {
		err := s.Store.PutBlob(ctx, r.Digest, r.Data)
		res := pb.BatchUpdateBlobsResponse_Response{}
		res.Digest = r.Digest
		if err != nil {
			res.Status = &status.Status{
				Code:    int32(code.Code_UNKNOWN),
				Message: fmt.Sprintf("Internal error %v", err),
			}
		} else {
			res.Status = &status.Status{
				Code:    int32(code.Code_OK),
				Message: "OK",
			}
		}
		response.Responses = append(response.Responses, &res)
	}
	return &response, nil
}

func (s *CASServer) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	log.Debugf("Recived BatchReadBlobs")
	return nil, nil
}

func (s *CASServer) GetTree(req *pb.GetTreeRequest, stream pb.ContentAddressableStorage_GetTreeServer) error {
	log.Debugf("Recived GetTree")
	return nil
}

func (s *CASServer) Read(req *pbbs.ReadRequest, stream pbbs.ByteStream_ReadServer) error {
	log.Debugf("Recived  Read")
	return nil
}

func (s *CASServer) Write(stream pbbs.ByteStream_WriteServer) error {
	log.Debugf("Recived  Write")
	data := bytes.NewBuffer(nil)
	digest := &pb.Digest{}
	for {
		d, err := stream.Recv()
		if d != nil {
			digest = resourceNameToDigest(d.ResourceName)
			data.Write(d.Data)
		}
		if err == io.EOF {
			err := s.Store.PutBlob(stream.Context(), digest, data.Bytes())
			if err != nil {
				return err
			}
			return stream.SendAndClose(
				&pbbs.WriteResponse{
					CommittedSize: digest.SizeBytes,
				},
			)
		}
		if err != nil {
			return err
		}
	}
}
func (s *CASServer) QueryWriteStatus(ctx context.Context, req *pbbs.QueryWriteStatusRequest) (*pbbs.QueryWriteStatusResponse, error) {
	log.Debugf("Recived  QueryWriteStatus")
	return nil, nil
}

func resourceNameToDigest(resourceName string) *pb.Digest {
	items := strings.Split(resourceName, "/")
	hash := items[len(items)-2]
	byte_size_str := items[len(items)-1]
	byte_size, _ := strconv.Atoi(byte_size_str)
	digest := pb.Digest{
		Hash:      hash,
		SizeBytes: int64(byte_size),
	}
	return &digest
}
