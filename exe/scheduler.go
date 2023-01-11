package exe

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/queue"
	"github.com/google/uuid"
)

type JobResultWaiter struct {
	JobID          string
	ActionResult   *pb.ActionResult
	NotifyChannels map[string]chan int
}

type Scheduler struct {
	queue       *queue.Queue
	RunningJobs map[string]*JobResultWaiter
	mu          sync.Mutex
}

func MakeScheduler() (*Scheduler, error) {
	s := Scheduler{
		queue:       &queue.Queue{},
		RunningJobs: map[string]*JobResultWaiter{},
		mu:          sync.Mutex{},
	}
	return &s, nil
}

func (s *Scheduler) Dispath(job *pb.Job) error {
	return s.queue.InsertJob(job)

}

func (s *Scheduler) Wait(job_id string) (*pb.ActionResult, error) {
	s.mu.Lock()
	waiter, ok := s.RunningJobs[job_id]
	channel_id := uuid.NewString()
	ch := make(chan int)
	if !ok {
		waiter = &JobResultWaiter{
			JobID: job_id,
			NotifyChannels: map[string]chan int{
				channel_id: ch,
			},
		}
		s.RunningJobs[job_id] = waiter
	} else {
		waiter.NotifyChannels[channel_id] = ch
	}

	s.mu.Unlock()
	// wait for action complete
	select {
	case <-time.After(10 * time.Second): // timeout
		s.onJobTimeout(job_id, channel_id)
	case <-ch:
		s.onJobComplete(job_id, channel_id)
	}

	return nil, nil
}

func (s *Scheduler) onJobTimeout(job_id, channel_id string) {
	fmt.Println("job timeout")
}

func (s *Scheduler) onJobComplete(job_id, channel_id string) {
	fmt.Printf("job complete %s", channel_id)
	s.mu.Lock()
	defer s.mu.Unlock()
	waiter, ok := s.RunningJobs[job_id]
	if ok {
		delete(waiter.NotifyChannels, channel_id)
		if len(waiter.NotifyChannels) == 0 {
			fmt.Println("delete the waiter")
			delete(s.RunningJobs, job_id)
		}
	}
}

func (s *Scheduler) Cancel(job_id string) error {
	return nil
}
