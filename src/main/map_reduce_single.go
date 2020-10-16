package main

import "fmt"
import "os"
import "io/ioutil"
import "sort"

// import "plugin"

import "log"
import "regexp"
import "strings"
import "strconv"

// single process MapReduce

// define key value pair
// ref: https://play.golang.org/p/aCKU5bTnhT
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

// ######################### UDFs ###########################
func mapFunction(filename string, text string) []KeyValue {
	// use regular expression to eliminate punctuations and symbols
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}
	processedString := reg.ReplaceAllString(text, " ")
	//seperate each word
	wordlist := strings.Fields(processedString)

	// create key value pairs for each word
	kvpair := []KeyValue{}
	for _, word := range wordlist {
		pair := KeyValue{word, "1"}
		kvpair = append(kvpair, pair)
	}
	return kvpair
}

func reduceFunction(key string, words []string) string {
	//count the occurrence of the word
	len := int64(len(words))
	//convert to string
	count := strconv.FormatInt(len, 10)
	return count
}
// #########################################################


// ########################### workers #####################
func mapWorker(inputFile string) []KeyValue {
	intermediate := []KeyValue{}
	data, err := ioutil.ReadFile(inputFile)
	if err != nil {
		fmt.Println("failed reading data from: " + inputFile)
		os.Exit(1)
	}
	// load: map function, reduce function
	// mapFunction, reduceFunction := load_map_reduce(os.Args[1])

	// call map function
	keyVal := mapFunction(inputFile, string(data))
	intermediate = append(intermediate, keyVal...)
	// sort by key
	sort.Sort(sortKey(intermediate))
	return intermediate
}

func reduceWorker(intermediate []KeyValue) {
	// output file
	output_file, _ := os.Create("output")

	// reduce function
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
		key_value := reduceFunction(curr_key, values_for_curr_key)

		fmt.Fprintf(output_file, "%v %v\n", curr_key, key_value)

		i = j
	}
	// output_file.close()
}


// ######################### master ##########################
func main() {
	// check input args, should be 3 args
	/*
	if len(os.Args) != 3 {
		fmt.Println("Correct Usage: map_reduce_single fileName.so(UDF) inputFile")
		os.Exit(1)
	}
	*/

	// read file, https://www.golangprograms.com/example-readall-readdir-and-readfile-from-io-package.html
	// get input file name
	// inputFile := os.Args[2]
	inputFile := os.Args[1]
	// intermediate container
	intermediate := mapWorker(inputFile)

	reduceWorker(intermediate)

	
}
