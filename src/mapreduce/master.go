package mapreduce

import (
	"fmt"
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

// start server to listen
func (master *Masterinfo) start_server() {
	rpc.Register(master)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", "127.0.0.1:1234")
	os.Remove("mapreduce")
	l, e := net.Listen("unix", "mapreduce")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// method for dealing with task asked from worker
func (master *Masterinfo) job_Distribute(arg bool, reply *job_Dist_Message) error {

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

// method for dealing with task report from worker
func (master *Masterinfo) job_Done(arg bool, finish *report_Message) error {

	if finish.IsDone == true {
		// job done
		master.jobStateQ[finish.TaskIndex].Status = "finish"
	} else {
		// job has error
		master.jobStateQ[finish.TaskIndex].Status = "error"
	}
	return nil
}

// check if the whole map reduce task is finished
func (master *Masterinfo) checkTaskFinished() bool {

	finished := false
	master.Mutex.Lock()
	defer master.Mutex.Unlock()

	for taskIndex, state := range master.jobStateQ {
		currStatus := state.Status
		if currStatus == "ready" {
			master.add_Job(taskIndex)
		} else if currStatus == "executing" {
			elapsed := time.Now().Sub(state.StartTime)
			fifteenSeconds, _ := time.ParseDuration("15s")
			// execute timeout
			if elapsed > fifteenSeconds {
				master.add_Job(taskIndex)
			}
		} else if currStatus == "finish" {
			// if current phase is map, the task is not done yet, call reduce_start()
			if master.MapOrRed == "map" {
				master.reduce_Start()
			} else { // if current phase is reduce and task is finished
				master.finish = true
				// close channel
				close(master.jobQ)
				finished = true
			}

		} else if currStatus == "error" {
			master.add_Job(taskIndex)
		} else { // other status ?
			fmt.Println("status exception")
		}
	}

	return finished
}

// initialize reduce
func (master *Masterinfo) reduce_Start() {
	// if map jobs are done, we start reduce jobs
	for i := range master.jobStateQ {
		master.jobStateQ[i].Status = "ready"
	}
	// change job type to reduce
	master.MapOrRed = "reduce"
	master.finish = false
	master.jobStateQ = make([]TaskState, master.N)

}

// put new job in job queue
func (master *Masterinfo) add_Job(task_Index int) {
	// initialize a job
	master.jobStateQ[task_Index].Status = "queue"
	job := Task{
		taskFile:     "",
		numMap:       len(master.filename),
		numReduce:    master.N,
		taskIndex:    task_Index,
		taskType:     master.MapOrRed,
		taskFinished: false,
	}
	if master.MapOrRed == "map" {
		job.taskFile = master.filename[task_Index]
	}
	// put the job into job queue
	master.jobQ <- job
}
