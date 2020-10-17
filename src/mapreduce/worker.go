package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

func askForTask() {

}

// worker: ask for tasks and report tasks
// parameters: map function and reduce function
func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	for {
		// request tasks

		// execute tasks

		// report tasks
	}
}

func executeTasks(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {

}

// ################### workers for executing tasks #####################
func mapWorker(mapFunction func(string, string) []KeyValue, inputFile string, mapTaskIndex int, numReduce int) {
	// intermediate := []KeyValue{}
	// read data from input file
	data, err := ioutil.ReadFile(inputFile)
	if err != nil {
		fmt.Println("map worker failed reading data from: " + inputFile)
		// exit
		os.Exit(1)
	}
	// load: map function, reduce function
	// mapFunction, reduceFunction := load_map_reduce(os.Args[1])

	// call UDF map function to produce key value pairs
	keyVals := mapFunction(inputFile, string(data))

	for i := 0; i < numReduce; i++ {
		// intermediate file, name rule: mapreduce_mapTaskIndex_i
		intermediateFile := "intermediate_" + strconv.Itoa(mapTaskIndex) + "_" + strconv.Itoa(i)
		fmt.Println("intermediate file name:" + intermediateFile)
		file, _ := os.Create(intermediateFile)
		encode := json.NewEncoder(file)
		for _, keyVal := range keyVals {
			//FIXME: unsigned int to int, may not work
			if int(hash(keyVal.Key))%numReduce == i {
				encode.Encode(&keyVal)
			}
		}
		file.Close()

	}

	// intermediate = append(intermediate, keyVal...)
	// sort by key
	// sort.Sort(sortKey(intermediate))
	// return
}

func reduceWorker(reduceFunction func(string, []string) string, numMap int, reduceTaskIndex int) {

	// container to store all key value pairs in intermediate file
	intermediate := []KeyValue{}
	// FIXME: redeclare i in the for loop below this one might cause problem?
	for i := 0; i < numMap; i++ {
		// get intermediate file name
		intermediateFile := "intermediate_" + strconv.Itoa(i) + "_" + strconv.Itoa(reduceTaskIndex)
		file, err := os.Open(intermediateFile)
		if err != nil {
			fmt.Println("reduce worker failed opening file: " + intermediateFile)
			// exit
			os.Exit(1)
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
}
