package main

import (
	"flag"
	"net"

	"github.com/expbuild/expbuild/pkg/ac"
	"github.com/expbuild/expbuild/pkg/cas"
	"github.com/expbuild/expbuild/pkg/cas/store"
	"github.com/expbuild/expbuild/pkg/exe"
	pbbs "github.com/expbuild/expbuild/pkg/proto/gen/bytestream"
	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/log"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:50051", "The exe server address")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer()
	exe_svr, err := exe.MakeExeServer()
	if err != nil {
		log.Errorf("exe server init error %v", err)
		panic(1)
	}

	cas_svr := cas.CASService{
		Store: store.MakeRedisStore(),
	}

	ac_svr := ac.NewActionCache()

	pb.RegisterContentAddressableStorageServer(s, &cas_svr)
	pbbs.RegisterByteStreamServer(s, &cas_svr)
	pb.RegisterExecutionServer(s, exe_svr)
	pb.RegisterActionCacheServer(s, ac_svr)
	exe_svr.Start()
	log.Infof("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Errorf("failed to serve: %v", err)
	}
}
