package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "log"
import "regexp"
import "strings"
import "strconv"

type KeyValue struct {
	Key   string
	Value string
}

func wcMap(filename string, text string) []KeyValue {
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

func wcReduce(key string, words []string) string {
	//count the occurrence of the word
	len := int64(len(words))
	//convert to string
	count := strconv.FormatInt(len, 10)
	return count
}
