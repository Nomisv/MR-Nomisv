package main

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"../mapreduce"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Correct usage: worker_main app.so")
		os.Exit(1)
	}
	// map, reduce functions loaded from os file
	mapFunction, reduceFunction := loadMapReduce(os.Args[1])
	// call worker in worker.go in /mapreduce
	mapreduce.Worker(mapFunction, reduceFunction)
}

// load
func loadMapReduce(fileName string) (func(string, string) []mapreduce.KeyValue, func(string, []string) string) {

	plug, err := plugin.Open(fileName)
	if err != nil {
		log.Fatalf("cannot load plugin %v %v", fileName, err)
	}
	// look for map function
	mapFunc, err := plug.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map Function in the file %v", fileName)
	}
	mapFunction := mapFunc.(func(string, string) []mapreduce.KeyValue)

	// look for reduce function
	reduceFunc, err := plug.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce Function in the file %v", fileName)
	}
	reduceFunction := reduceFunc.(func(string, []string) string)

	return mapFunction, reduceFunction
}
