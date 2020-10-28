package main

// use the following command to turn wc.go to wc.so
// go build -buildmode=plugin wc.go

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"

	"../mapreduce"

	crand "crypto/rand"
)

func fault() {
	max := big.NewInt(100)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 33 {
		fmt.Print("worker crash")
		os.Exit(1)
	}

}

func Map(filename string, text string) []mapreduce.KeyValue {
	// crash happens
	fault()
	// use regular expression to eliminate punctuations and symbols
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}
	processedString := reg.ReplaceAllString(text, " ")
	//seperate each word
	wordlist := strings.Fields(processedString)

	// create key value pairs for each word
	kvpair := []mapreduce.KeyValue{}
	for _, word := range wordlist {
		pair := mapreduce.KeyValue{word, "1"}
		kvpair = append(kvpair, pair)
	}
	return kvpair
}

func Reduce(key string, words []string) string {
	//count the occurrence of the word
	len := int64(len(words))
	//convert to string
	count := strconv.FormatInt(len, 10)
	return count
}
