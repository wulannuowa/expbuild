package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/expbuild/expbuild/cas"
	"github.com/expbuild/expbuild/cas/store"
	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	svr := cas.CASServer{
		Store: store.MakeRedisStore(),
	}
	pb.RegisterContentAddressableStorageServer(s, &svr)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
