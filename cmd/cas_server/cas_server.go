package main

import (
	"flag"
	"net"

	"github.com/expbuild/expbuild/pkg/cas"
	"github.com/expbuild/expbuild/pkg/cas/store"
	pbbs "github.com/expbuild/expbuild/pkg/proto/gen/bytestream"
	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/log"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:50051", "The server address")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer()
	svr := cas.CASService{
		Store: store.MakeRedisStore(),
	}
	pb.RegisterContentAddressableStorageServer(s, &svr)
	pbbs.RegisterByteStreamServer(s, &svr)
	log.Infof("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Errorf("failed to serve: %v", err)
	}
}
