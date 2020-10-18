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
	// task queue, use chan data structure
	TaskChan chan Task
	// files (input file will be splitted)
	Files []string
	// number of map, should be N
	MapNum int
	// number of reduce, should be N
	ReduceNum int
	// task phase: map / reduce
	TaskPhase string
	// task state for each task
	TaskState []TaskState
	// mutex lock
	Mutex sync.Mutex
	// check if tasks are done
	IsDone bool
}

type TaskState struct {
	// status: ready, queue, executing, finish, error
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
	master.IsDone = false
	master.Files = input_File
	master.MapNum = len(input_File)
	master.ReduceNum = reduce_n
	master.TaskPhase = "map"
	master.TaskState = make([]TaskState, master.MapNum)
	master.TaskChan = make(chan Task, 10)
	for k := range master.TaskState {
		master.TaskState[k].Status = "ready"
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

	job, ok := <-master.TaskChan
	if ok == true {
		reply.Task = job
		// job executing
		master.TaskState[job.taskIndex].Status = "executing"
		// job time record
		master.TaskState[job.taskIndex].StartTime = time.Now()
	} else {
		// check if there is any other job to distribute
		reply.TaskDone = true
	}

	return nil
}

func (master *Masterinfo) job_Done(finish *report_Message) error {

	if finish.IsDone == true {
		// job done
		master.TaskState[finish.TaskIndex].Status = "finish"
	} else {
		// error
		master.TaskState[finish.TaskIndex].Status = "error"
	}
	return nil
}

/*
func (master *Master) mapFinished() bool {
	return true
}

func (master *Master) addToQueue(taskIndex int) {

}
*/
