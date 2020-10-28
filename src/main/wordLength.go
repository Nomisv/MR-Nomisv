package main


// go build -buildmode=plugin wordLength.go

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"../mapreduce"
)

//  this mapreduce function will find the length of each word

func Map(filename string, text string) []mapreduce.KeyValue {
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
		//each word is a key
		pair := mapreduce.KeyValue{word, string(len(word))}
		kvpair = append(kvpair, pair)
	}
	return kvpair
}



func Reduce(key string, words []string) string {
	// return the length of string
	return strconv.Itoa(len(key))
}