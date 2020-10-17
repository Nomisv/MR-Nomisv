package mapreduce

import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

// ref: https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func askForTask() {

}

// worker: ask for tasks and report tasks
// parameters: map function and reduce function
func Worker(mapfunction func(string, string) []KeyValue, reducefunction func(string, []string) string) {
	for {
		// request tasks

	}
}
