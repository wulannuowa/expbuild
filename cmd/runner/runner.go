package main

// a simple runner

import (
	"flag"
	"os"

	"github.com/expbuild/expbuild/bazel-expbuild/pkg/util/file"
	"github.com/expbuild/expbuild/bazel-expbuild/pkg/util/log"
	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/golang/protobuf/proto"
)

var (
	rabbitmq_conn_str = flag.String("consume_job_from_amqp", "amqp://guest:guest@localhost:5672/%2F", "The address of message queue [rabbitmq]")
	job_file          = flag.String("consume_job_from_file", "", "read serialized job from a file")
)

// read a job from file
func readJobFromFile(f string) (job *pb.Job, err error) {
	isExist, err := file.FileExists(f)
	if !isExist {
		log.Errorf("file %s not exist", f)
		return nil, err
	}

	data, err := file.ReadFile(f)
	if err != nil {
		log.Errorf("read file %s error: %v", f, err)
		return nil, err
	}

	var j pb.Job
	proto.Unmarshal(data, &j)
	return &j, nil
}

func prepareWorkSpace() {

}

func runJob(job *pb.Job) {

}
func main() {
	flag.Parse()
	log.Infof("runner started")
	// read job from file
	if *job_file != "" {
		job, err := readJobFromFile(*job_file)
		if err != nil {
			log.Errorf("read job from file error: %v", err)
			os.Exit(1)
		}
		runJob(job)
	}
}
