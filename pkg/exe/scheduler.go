package exe

// A job scheduler based on amqp

import (
	"flag"
	"fmt"
	"sync"
	"time"

	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/log"
	"github.com/golang/protobuf/proto"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	job_queue_name       = "job-queue"
	result_exchange_name = "job-result-exchange"
)

var (
	rabbitmq_conn_str = flag.String("amqp", "amqp://guest:guest@localhost:5672/%2F", "The address of message queue [rabbitmq]")
)

// The scheduler which manage the job queue
type Scheduler struct {
	// All the running jobs, it'll be deleted when the job is complete
	RunningJobs map[string]*JobResultWaiter // job_id -> JobResultWaiter
	queue       *JobDisQueue
	mu          sync.Mutex
}

// The Job dipatch queue implemented by amqp
type JobDisQueue struct {
	scheduler      *Scheduler
	conn           *amqp.Connection
	channel        *amqp.Channel
	jobQueue       *amqp.Queue
	jobResultQueue *amqp.Queue
}

// a job result waiter which wait the job complete or timeout
type JobResultWaiter struct {
	JobID         string
	ActionResult  *pb.ActionResult
	NotifyChannel chan int
}

func MakeScheduler() (*Scheduler, error) {
	s := Scheduler{
		RunningJobs: map[string]*JobResultWaiter{},
		mu:          sync.Mutex{},
	}
	queue := &JobDisQueue{
		scheduler: &s,
	}
	err := queue.Init()
	if err != nil {
		return nil, err
	}
	s.queue = queue

	return &s, nil
}

func (s *Scheduler) Dispatch(job *pb.Job) error {
	return s.queue.SendJob(job)

}

// get the job from scheduler
func (s *Scheduler) GetJobActionResult(job_id string) (*pb.ActionResult, error) {
	s.mu.Lock()

	defer s.mu.Unlock()
	waiter, ok := s.RunningJobs[job_id]
	if ok {
		return waiter.ActionResult, nil
	}
	return nil, fmt.Errorf("job not found %s", job_id)
}

func (s *Scheduler) Wait(job_id string) error {
	s.mu.Lock()
	_, ok := s.RunningJobs[job_id]
	if ok {
		return fmt.Errorf("job has been in waiting , job_id: %s", job_id)
	}
	ch := make(chan int)

	waiter := &JobResultWaiter{
		JobID:         job_id,
		NotifyChannel: ch,
	}
	s.RunningJobs[job_id] = waiter

	s.mu.Unlock()
	// wait for action complete
	select {
	case <-time.After(10 * time.Second): // timeout
		return fmt.Errorf("job timeout %s", job_id)
	case <-ch:
		return nil
	}
}

// when the job complete, delete the channel
func (s *Scheduler) DeleteJob(job_id string) {
	//fmt.Printf("job complete %s", job_id)
	log.Debugf("job complete %s", job_id)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.RunningJobs[job_id]
	if ok {
		delete(s.RunningJobs, job_id)
	}
}

func (s *Scheduler) Cancel(job_id string) error {
	return nil
}

// when the job complete as normal
func (s *Scheduler) OnJobComplete(job_id string, action_result *pb.ActionResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiter, ok := s.RunningJobs[job_id]
	if ok {
		waiter.ActionResult = action_result
		waiter.NotifyChannel <- 1
		return
	}
	//fmt.Printf("job complete %s", job_id)
	log.Debugf("job complete %s", job_id)
}

// init the job dispatch queue
func (q *JobDisQueue) Init() error {
	conn, err := amqp.Dial(*rabbitmq_conn_str)
	if err != nil {
		log.Errorf("Connecting to %s error %v", *rabbitmq_conn_str, err)
		return err
	}
	q.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("Create rabbitmq channel error %v", err)
		return err
	}
	q.channel = ch

	if err := q.declearJobQueue(); err != nil {
		return err
	}
	if err := q.declearJobResultQueue(); err != nil {
		return err
	}
	return nil
}

// Job queue is just a simple queue
func (q *JobDisQueue) declearJobQueue() error {
	job_queue, err := q.channel.QueueDeclare(job_queue_name, true, false, false, false, nil)

	if err != nil {
		log.Errorf("Rabbitmq declare job queue error %v", err)
		return err
	}
	q.jobQueue = &job_queue
	return nil
}

// Job Result queue is a fanout exchange
func (q *JobDisQueue) declearJobResultQueue() error {
	if err := q.channel.ExchangeDeclare(result_exchange_name, "fanout", true, false, false, false, nil); err != nil {
		log.Errorf("Rabbitmq declear exchange error %v", err)
		return err
	}

	queue, err := q.channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		log.Errorf("Rabbitmq declear queue error %v", err)
	}

	if err := q.channel.QueueBind(queue.Name, "", result_exchange_name, false, nil); err != nil {
		log.Errorf("Rabbitmq queue binding error %v", err)
		return err
	}
	q.jobResultQueue = &queue
	return nil
}

// send job to job queue
func (q *JobDisQueue) SendJob(job *pb.Job) error {
	body, err := proto.Marshal(job)
	if err != nil {
		return err
	}

	if err := q.channel.Publish("", q.jobQueue.Name, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Priority:     1,
		Timestamp:    time.Now(),
		AppId:        "expbuild",
		Type:         "job",
	}); err != nil {
		log.Errorf("Rabbitmq send job error %v", err)
		return err
	}
	return nil
}

// start a new goroutine to consume result from job result queue
func (q *JobDisQueue) StartConsume() error {

	msgs, err := q.channel.Consume(q.jobResultQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Errorf("Rabbitmq consume error %v", err)
		return err
	}
	var forever chan struct{}
	go func() {
		for d := range msgs {
			job := pb.Job{}
			proto.Unmarshal(d.Body, &job)
			q.scheduler.OnJobComplete(job.Id, job.Result)
		}
	}()
	<-forever
	return nil
}
