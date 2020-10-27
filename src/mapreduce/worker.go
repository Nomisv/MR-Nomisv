package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type KeyValue struct {
	Key   string
	Value string
}

// sort by key
// ref: https://golang.org/pkg/sort/
// sort.Interface
type sortKey []KeyValue

// Len
func (keyVal sortKey) Len() int {
	return len(keyVal)
}

// Swap
func (keyVal sortKey) Swap(i, j int) {
	keyVal[i], keyVal[j] = keyVal[j], keyVal[i]
}

// Less
func (keyVal sortKey) Less(i, j int) bool {
	return keyVal[i].Key < keyVal[j].Key
}

// ref: https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// ref: https://golang.org/pkg/net/rpc/

func askForTask() Job_Dist_Message {
	// use rpc
	//FIXME: might cause problem
	client, err := rpc.DialHTTP("unix", "mapreduce")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	askMsg := Job_Dist_Message{}
	var reply bool
	err = client.Call("Masterinfo.JobDistribute", &reply, &askMsg)
	if err != nil {
		log.Fatal("failed ask for task", err)
	}
	return askMsg
}

func reportTask(taskIndex int, finish bool) Report_Message {
	//FIXME: might cause problem
	reportMsg := Report_Message{}
	reportMsg.IsDone = finish
	reportMsg.TaskIndex = taskIndex
	// dial
	client, err := rpc.DialHTTP("unix", "mapreduce")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// rpc
	var reply bool
	err = client.Call("Masterinfo.JobDone", &reportMsg, &reply)
	if err != nil {
		log.Fatal("failed report task", err)
	}
	// test
	fmt.Println("report message, task index:", reportMsg.TaskIndex, "is done?", reportMsg.IsDone)

	return reportMsg
}

// worker: ask for tasks and report tasks
// parameters: map function and reduce function
func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	for {
		// request tasks
		askMsg := askForTask()
		if askMsg.TaskDone == true {
			// the whole map reduce task is done, we dont need to request task anymore
			fmt.Println("all work done")
			break
		}
		// execute tasks and report false if mission failed
		if executeTasks(mapFunction, reduceFunction, askMsg.Task) == false {
			fmt.Println("failed execute task", askMsg.Task.TaskIndex)
			reportTask(askMsg.Task.TaskIndex, false)
		}
		// report tasks succeed if mission complete
		reportTask(askMsg.Task.TaskIndex, true)

	}
}

func executeTasks(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string, task Task) bool {
	if task.TaskType == "map" {
		if mapWorker(mapFunction, task.TaskFile, task.TaskIndex, task.NumReduce) == false {
			return false
		}
	} else if task.TaskType == "reduce" {
		if reduceWorker(reduceFunction, task.NumMap, task.TaskIndex) == false {
			return false
		}
	} else {
		fmt.Println("executeTasks error")
		return false
	}
	return true
}

// ################### workers for executing tasks #####################
func mapWorker(mapFunction func(string, string) []KeyValue, inputFile string, mapTaskIndex int, numReduce int) bool {
	// intermediate := []KeyValue{}
	// read data from input file

	/*
		data, err := ioutil.ReadFile(inputFile)
		if err != nil {
			fmt.Println("map worker failed reading data from: " + inputFile)
			// exit
			return false
		}
	*/

	// ---------------------- get block --------
	datas := "1"
	f, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("map worker failed reading data from: " + inputFile)
	} else {
		//get the size of the file
		//get the size of the file
		n, _ := f.Seek(0, 2)
		size := int(n) / numReduce
		datas = ""
		start := size * mapTaskIndex
		loc := start

		for true {
			bs := make([]byte, size)

			f.ReadAt(bs, int64(loc))

			datas = string(bs)
			if loc == 0 {
				break
			}
			if string(bs[0]) == "\n" {
				break
			}
			if string(bs[0]) == " " {
				break
			}
			datas = string(bs)
			loc++
			size--
		}
		for true {
			bs := make([]byte, size)
			_, err := f.ReadAt(bs, int64(loc))
			datas = string(bs)
			if err == io.EOF {
				break
			}
			if string(bs[len(bs)-1]) == "\n" {
				break
			}
			if string(bs[len(bs)-1]) == " " {
				break
			}
			datas = string(bs)
			size++

		}
	}

	// ---------------------------------------

	// load: map function, reduce function
	// mapFunction, reduceFunction := load_map_reduce(os.Args[1])

	// call UDF map function to produce key value pairs
	// keyVals := mapFunction(inputFile, string(data))
	keyVals := mapFunction(inputFile, datas)
	// fmt.Println(numReduce)
	for i := 0; i < numReduce; i++ {
		// intermediate file, name rule: mapreduce_mapTaskIndex_i
		intermediateFile := "intermediate_map" + strconv.Itoa(mapTaskIndex) + "_reduce" + strconv.Itoa(i)
		fmt.Println("intermediate file name:" + intermediateFile)
		file, err := os.Create(intermediateFile)
		if err != nil {
			fmt.Println("failed create file")
			return false
		}
		encode := json.NewEncoder(file)
		for _, keyVal := range keyVals {
			//FIXME: unsigned int to int, may not work
			if int(hash(keyVal.Key))%numReduce == i {
				encode.Encode(&keyVal)
			}
		}
		file.Close()

	}
	return true
	// intermediate = append(intermediate, keyVal...)
	// sort by key
	// sort.Sort(sortKey(intermediate))
	// return
}

func reduceWorker(reduceFunction func(string, []string) string, numMap int, reduceTaskIndex int) bool {
	fmt.Println("reduce worker working")
	// container to store all key value pairs in intermediate file
	intermediate := []KeyValue{}
	// FIXME: redeclare i in the for loop below this one might cause problem?
	for i := 0; i < numMap; i++ {
		// get intermediate file name
		intermediateFile := "intermediate_map" + strconv.Itoa(i) + "_reduce" + strconv.Itoa(reduceTaskIndex)
		file, err := os.Open(intermediateFile)
		if err != nil {
			fmt.Println("reduce worker failed opening file: " + intermediateFile)
			// exit
			// os.Exit(1)
			return false
		}
		// decode file
		decode := json.NewDecoder(file)
		for {
			var keyVal KeyValue
			if err := decode.Decode(&keyVal); err != nil {
				fmt.Println("decode error")
				break
			}
			// append key value to intermediate container
			intermediate = append(intermediate, keyVal)
		}
		file.Close()
	}
	// sort by key
	sort.Sort(sortKey(intermediate))

	// output file
	name := "output" + strconv.Itoa(reduceTaskIndex)
	outputFile, _ := os.Create(name)
	i := 0
	// each key
	for i < len(intermediate) {

		curr_key := intermediate[i].Key
		values_for_curr_key := []string{}
		// append the first occurence
		values_for_curr_key = append(values_for_curr_key, intermediate[i].Value)
		// since keys are sorted, we could iterate through intermediate in order
		j := i + 1
		for j < len(intermediate) {
			if intermediate[j].Key == curr_key {
				values_for_curr_key = append(values_for_curr_key, intermediate[j].Value)
				j++
			} else {
				break
			}
		}
		//TODO: reduce function
		key_value := reduceFunction(curr_key, values_for_curr_key)

		fmt.Fprintf(outputFile, "%v %v\n", curr_key, key_value)

		i = j
	}
	outputFile.Close()
	return true
}
