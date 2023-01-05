package exe

import (
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/util/log"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	longrunning "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	JobQueueName       = "job-queue"
	ResultExchangeName = "job-result-exchange"
)

type jobWaiter struct {
	Job      *pb.Job
	Notifier chan int
}

type ExeServer struct {
	pb.UnimplementedExecutionServer
	Amqp        string
	qconn       *amqp.Connection
	channel     *amqp.Channel
	job_queue   *amqp.Queue
	runningJobs map[string]*jobWaiter
}

func (s *ExeServer) Init() error {
	conn, err := amqp.Dial(s.Amqp)
	if err != nil {
		log.Errorf("Connecting to %s error %v", s.Amqp, err)
		return err
	}
	s.qconn = conn
	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("Create rabbitmq channel error %v", err)
		return err
	}
	s.channel = ch
	job_queue, err := ch.QueueDeclare(JobQueueName, true, false, false, false, nil)

	if err != nil {
		log.Errorf("Rabbitmq declare queue error %v", err)
		return err
	}
	s.job_queue = &job_queue

	s.runningJobs = make(map[string]*jobWaiter)
	go s.consumeResult()
	return nil
}

func (s *ExeServer) Execute(req *pb.ExecuteRequest, stream pb.Execution_ExecuteServer) error {
	log.Debugf("recived execute request %v", req.ActionDigest)
	job := pb.Job{
		Id:     uuid.NewString(),
		Action: req.ActionDigest,
	}
	body, err := proto.Marshal(&job)
	if err != nil {
		log.Errorf("marshal job error %v", err)
		return err
	}

	s.channel.PublishWithContext(stream.Context(), "", s.job_queue.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})

	waiter := &jobWaiter{
		Job:      &job,
		Notifier: make(chan int),
	}

	//TODO lock
	s.runningJobs[job.Id] = waiter

	<-waiter.Notifier
	//TODO remove the waiter
	res, err := anypb.New(job.Result)
	if err != nil {
		return fmt.Errorf("recived a wrong job result")
	}
	operation := &longrunning.Operation{
		Name: job.Id,
		Done: true,
		Result: &longrunningpb.Operation_Response{
			Response: res,
		},
	}
	return stream.Send(operation)
	//return status.Errorf(codes.Unimplemented, "method Execute not implemented")
}
func (s *ExeServer) WaitExecution(req *pb.WaitExecutionRequest, stream pb.Execution_WaitExecutionServer) error {
	return status.Errorf(codes.Unimplemented, "method WaitExecution not implemented")
}

func (s *ExeServer) consumeResult() error {
	err := s.channel.ExchangeDeclare(ResultExchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorf("exchange declear error %v", err)
		return err
	}
	q, err := s.channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		log.Errorf("queue declear error %v", err)
		return err
	}
	err = s.channel.QueueBind(q.Name, "", ResultExchangeName, false, nil)
	if err != nil {
		log.Errorf("queue binding error %v", err)
		return err
	}
	msgs, err := s.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Errorf("consume msg error %v", err)
		return err
	}

	var forever chan struct{}
	go func() {
		for d := range msgs {
			job := pb.Job{}
			proto.Unmarshal(d.Body, &job)
			waiter := s.runningJobs[job.Id]
			waiter.Notifier <- 1 // notify
		}
	}()
	<-forever
	return nil
}
