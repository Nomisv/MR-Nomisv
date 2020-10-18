package mapreduce

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// master
type Masterinfo struct {

	// number of reduce
	N int

	filename []string

	// is mapping or reducing
	MapOrRed string

	// job queue
	jobQ chan Task

	// job status queue
	jobStateQ []TaskState

	// lock
	Mutex sync.Mutex

	// job finished
	finish bool
}

type TaskState struct {
	// status: executing, error ...
	Status string
	// starting time
	StartTime time.Time
}

func master_Start(reduce_n int, input_File []string) *Masterinfo {

	master := Masterinfo{}
	//initialize the master with input parameters
	master.initial_Master(reduce_n, input_File)
	// start master server
	master.start_server()

	return &master
}

func (master *Masterinfo) initial_Master(reduce_n int, input_File []string) {
	// initialize the Master
	master.finish = false
	master.filename = input_File
	master.N = reduce_n
	master.MapOrRed = "map"
	master.jobStateQ = make([]TaskState, master.N)
	master.jobQ = make(chan Task, 10)
	for k := range master.jobStateQ {
		master.jobStateQ[k].Status = "ready"
	}
}

func (master *Masterinfo) start_server() {
	rpc.Register(master)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", "127.0.0.1:1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (master *Masterinfo) job_Distribute(reply *job_Dist_Message) error {

	job, ok := <-master.jobQ
	if ok == true {
		reply.Task = job
		// job running
		master.jobStateQ[job.taskIndex].Status = "executing"
		// job time record
		master.jobStateQ[job.taskIndex].StartTime = time.Now()
	} else {
		// check if there is any other job to distribute
		reply.TaskDone = true
	}

	return nil
}

func (master *Masterinfo) job_Done(finish *report_Message) error {

	if finish.IsDone == true {
		// job done
		master.jobStateQ[finish.TaskIndex].Status = "finish"
	} else {
		// job has error
		master.jobStateQ[finish.TaskIndex].Status = "error"
	}
	return nil
}
