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

	filename string

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

func Master_Start(reduce_n int, input_File string) *Masterinfo {

	//initialize the master with input parameters
	master := Initial_Master(reduce_n, input_File)
	// start master server
	master.Start_server()

	return &master
}

func Initial_Master(reduce_n int, input_File string) Masterinfo {
	// initialize the Master
	master := Masterinfo{}
	master.finish = false
	master.filename = input_File
	master.N = reduce_n
	master.MapOrRed = "map"
	master.jobStateQ = make([]TaskState, master.N)
	master.jobQ = make(chan Task, master.N)
	for k := range master.jobStateQ {
		master.jobStateQ[k].Status = "ready"
	}
	return master
}

// start server to listen
func (master *Masterinfo) Start_server() {
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
func (master *Masterinfo) JobDistribute(arg *bool, reply *Job_Dist_Message) error {

	job, ok := <-master.jobQ
	if ok == true {
		reply.Task = job
		// job running
		master.jobStateQ[job.TaskIndex].Status = "executing"
		// job time record
		master.jobStateQ[job.TaskIndex].StartTime = time.Now()
	} else {
		// check if there is any other job to distribute
		reply.TaskDone = true
	}

	return nil
}

// method for dealing with task report from worker
func (master *Masterinfo) JobDone(finish *Report_Message, arg *bool) error {
	// test
	fmt.Println("received report message, task index:", finish.TaskIndex, "is done?", finish.IsDone)
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
func (master *Masterinfo) CheckTaskFinished() bool {

	finished := false
	finishedTasks := 0
	master.Mutex.Lock()
	defer master.Mutex.Unlock()
	// check each task by taskIndex
	for taskIndex, state := range master.jobStateQ {
		currStatus := state.Status
		fmt.Println("current task:", taskIndex, currStatus)
		if currStatus == "ready" {
			master.add_Job(taskIndex)
			fmt.Println(taskIndex, "job added to queue")
		} else if currStatus == "executing" {
			elapsed := time.Now().Sub(state.StartTime)
			fifteenSeconds, _ := time.ParseDuration("15s")
			// execute timeout
			if elapsed > fifteenSeconds {
				master.add_Job(taskIndex)
			}
		} else if currStatus == "finish" {
			finishedTasks++
		} else if currStatus == "error" {
			master.add_Job(taskIndex)
		} else if currStatus == "queue" {
			fmt.Println("task in queue", taskIndex)
		} else { // other status ?
			fmt.Println(currStatus + "status exception")
		}
	}
	// if the number of finished tasks == N, current phase complete
	if finishedTasks == master.N {
		if master.MapOrRed == "map" {
			// if current phase is map, the task is not done yet, call reduce_start()
			master.reduce_Start()
		} else if master.MapOrRed == "reduce" {
			// if current phase is reduce and tasks are finished
			master.finish = true
			// close channel
			close(master.jobQ)
			finished = true
		} else {
			log.Fatalf("master phase error")
		}
	}

	return finished
}

// initialize reduce
func (master *Masterinfo) reduce_Start() {
	// if map jobs are done, we start reduce jobs
	fmt.Println("start reduce")
	for i := range master.jobStateQ {
		master.jobStateQ[i].Status = "ready"
	}
	// change job type to reduce
	master.MapOrRed = "reduce"
	master.finish = false
	// master.jobStateQ = make([]TaskState, master.N)

}

// put new job in job queue
func (master *Masterinfo) add_Job(task_Index int) {
	// initialize a job
	master.jobStateQ[task_Index].Status = "queue"
	job := Task{
		TaskFile:     "",
		NumMap:       master.N,
		NumReduce:    master.N,
		TaskIndex:    task_Index,
		TaskType:     master.MapOrRed,
		TaskFinished: false,
	}
	if master.MapOrRed == "map" {
		job.TaskFile = master.filename
	}
	// put the job into job queue
	master.jobQ <- job
}
