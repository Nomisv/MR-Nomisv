package mapreduce

type Task struct {
	// task type, map or reduce
	taskType string
	// task file name
	taskFile string
	// number of map, in this assignemnt numMap = numReduce
	numMap int
	// number of reduce
	numReduce int
	// task status: finished or not
	taskFinished bool
	// task index
	taskIndex int
}

// ref: https://golang.org/pkg/net/rpc/#Call

type job_Dist_Message struct {
	Task     Task
	TaskDone bool
}

type report_Message struct {
	TaskIndex int
	IsDone    bool
}
