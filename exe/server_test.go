package exe

import (
	"testing"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	longrunning "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
)

type mockExecutionStream struct {
	grpc.ServerStream
}

func (s mockExecutionStream) Send(op *longrunning.Operation) error {
	return nil
}

func TestExeServer_Execute(t *testing.T) {
	exe_svr := ExeServer{
		Amqp: "amqp://guest:guest@localhost:5672/%2F",
	}
	if err := exe_svr.Init(); err != nil {
		t.Errorf("exe server init error %v", err)
		panic(1)
	}
	req := pb.ExecuteRequest{
		ActionDigest: &pb.Digest{
			Hash:      "hashtest",
			SizeBytes: 8,
		},
	}

	go func() {

	}()
	stream := mockExecutionStream{}
	exe_svr.Execute(&req, stream)

}
