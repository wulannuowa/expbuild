package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")
	cmd        = flag.String("command", "help", "The command to execute [uploadfile, uploaddir, downloadfile, downloaddir]")
)

func findMissing(client pb.ContentAddressableStorageClient, ctx context.Context, digests []*pb.Digest) {
	req := pb.FindMissingBlobsRequest{
		BlobDigests: []*pb.Digest{
			{
				Hash:      "test",
				SizeBytes: 123,
			},
		},
	}

	r, err := client.FindMissingBlobs(ctx, &req)
	if err != nil {
		log.Errorf("some thing getting error %v", err)
	}
	fmt.Println(r.String())
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Errorf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewContentAddressableStorageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	findMissing(client, ctx, []*pb.Digest{})
}
