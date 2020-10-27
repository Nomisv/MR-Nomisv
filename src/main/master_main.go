package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"../mapreduce"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Correct usage: master_main inputFileName N")
		os.Exit(1)
	}
	// call master.go in /mapreduce
	N, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("argument type incorrect")
	}
	master := mapreduce.Master_Start(N, os.Args[1])

	// check if task is done
	for master.CheckTaskFinished() == false {
		time.Sleep(time.Second)
	}
	// all map reduce tasks finished
	fmt.Println("map reduce task finished")
}
