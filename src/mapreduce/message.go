package mapreduce

type Task struct {
	// task type, map or reduce
	TaskType string
	// task file name
	TaskFile string
	// number of map, in this assignemnt numMap = numReduce
	NumMap int
	// number of reduce
	NumReduce int
	// task status: finished or not
	TaskFinished bool
	// task index
	TaskIndex int
}

// ref: https://golang.org/pkg/net/rpc/#Call

type Job_Dist_Message struct {
	Task     Task
	TaskDone bool
}

type Report_Message struct {
	TaskIndex int
	IsDone    bool
}
