package mapreduce

type Task struct {
	// task type, map or reduce
	taskType string
	// task file name
	taskFile string
	// number of map
	numMap int
	// number of reduce
	numReduce int
	// task status: finished or not
	taskFinished bool
	// task index
	taskIndex int
}
