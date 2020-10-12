package main

import "fmt"
import "os"
import "sort"

// single process MapReduce

// define key value pair
// ref: https://play.golang.org/p/aCKU5bTnhT
type KeyValue struct {
	Key string
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
func (keyVal sortKey) Less(i, j, bool) bool {
	return keyVal[i].Key < keyVal[j].Key 
}

// main function
func main() {
	// check input args 
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Correct Usage: map_reduce_single fileName.so inputFile \n")
		os.Exit(1)
	}
}