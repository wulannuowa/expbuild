package exe

import (
	"fmt"

	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/log"
	"github.com/google/uuid"
	longrunning "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type ExeServer struct {
	pb.UnimplementedExecutionServer
	Amqp      string
	scheduler *Scheduler
}

func MakeExeServer() (*ExeServer, error) {
	scheduler, err := MakeScheduler()
	if err != nil {
		return nil, err
	}
	return &ExeServer{scheduler: scheduler}, nil
}

func (s *ExeServer) Start() {
	go func() {
		s.scheduler.queue.StartConsume()
	}()
}

func (s *ExeServer) Execute(req *pb.ExecuteRequest, stream pb.Execution_ExecuteServer) error {
	log.Debugf("recived execute request %v", req.ActionDigest)
	job := pb.Job{
		Id:     uuid.NewString(),
		Action: req.ActionDigest,
	}
	s.scheduler.Dispatch(&job)
	err := s.scheduler.Wait(job.Id)
	//TODO result
	if err != nil {
		// construct error action result
	}

	action_result, err := s.scheduler.GetJobActionResult(job.Id)
	if err != nil {
		return err
	}
	defer s.scheduler.DeleteJob(job.Id)

	res, err := anypb.New(action_result)
	if err != nil {
		return fmt.Errorf("recived a wrong job result")
	}
	operation := &longrunning.Operation{
		Name: job.Id,
		Done: true,
		Result: &longrunning.Operation_Response{
			Response: res,
		},
	}
	return stream.Send(operation)
	//return status.Errorf(codes.Unimplemented, "method Execute not implemented")
}
func (s *ExeServer) WaitExecution(req *pb.WaitExecutionRequest, stream pb.Execution_WaitExecutionServer) error {
	return status.Errorf(codes.Unimplemented, "method WaitExecution not implemented")
}
