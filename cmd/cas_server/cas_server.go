package main

import (
	"flag"
	"fmt"
	"net"

	"github.com/expbuild/expbuild/cas"
	"github.com/expbuild/expbuild/cas/store"
	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/util/log"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Errorf("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer()
	svr := cas.CASServer{
		Store: store.MakeRedisStore(),
	}
	pb.RegisterContentAddressableStorageServer(s, &svr)
	log.Infof("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Errorf("failed to serve: %v", err)
	}
}
