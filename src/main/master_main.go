package main

import (
	"fmt"
	"os"
	"time"

	"../mapreduce"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Correct usage: master_main inputFileName.")
		os.Exit(1)
	}
	// call master.go in /mapreduce
	master := mapreduce.Master_Start(2, os.Args[1])

	// check if task is done
	for master.CheckTaskFinished() == false {
		time.Sleep(time.Second)
	}
	// all map reduce tasks finished
	fmt.Println("map reduce task finished")
}
