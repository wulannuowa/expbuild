package queue

import (
	"flag"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/util/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	JobQueueName       = "job-queue"
	ResultExchangeName = "job-result-exchange"
)

var (
	rabbitmq_conn_str = flag.String("amqp", "amqp://guest:guest@localhost:5672/%2F", "The address of message queue [rabbitmq]")
)

type Queue struct {
	serverID  string
	qconn     *amqp.Connection
	channel   *amqp.Channel
	job_queue *amqp.Queue
}

func (q *Queue) Init(serverID string) error {
	q.serverID = serverID
	conn, err := amqp.Dial(*rabbitmq_conn_str)
	if err != nil {
		log.Errorf("Connecting to %s error %v", *rabbitmq_conn_str, err)
		return err
	}
	q.qconn = conn
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
func (q *Queue) declearJobQueue() error {
	job_queue, err := q.channel.QueueDeclare(JobQueueName, true, false, false, false, nil)

	if err != nil {
		log.Errorf("Rabbitmq declare job queue error %v", err)
		return err
	}
	q.job_queue = &job_queue
	return nil
}

// Job Result queue is a fanout exchange
func (q *Queue) declearJobResultQueue() error {
	if err := q.channel.ExchangeDeclare(ResultExchangeName, "fanout", true, false, false, false, nil); err != nil {
		log.Errorf("Rabbitmq declear exchange error %v", err)
		return err
	}

	queue, err := q.channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		log.Errorf("Rabbitmq declear queue error %v", err)
	}

	if err := q.channel.QueueBind(queue.Name, "", ResultExchangeName, false, nil); err != nil {
		log.Errorf("Rabbitmq queue binding error %v", err)
		return err
	}
	return nil

}

func (q *Queue) Start() error {

	return nil
}

func (q *Queue) InsertJob(job *pb.Job) error {
	return nil
}
