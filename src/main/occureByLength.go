package main


// go build -buildmode=plugin occureByLength.go

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"../mapreduce"
)

//  this mapreduce function will find the number of occurence of certain length of word

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
		// each length of word is a key
		pair := mapreduce.KeyValue{strconv.Itoa(len(word)), "1"}
		kvpair = append(kvpair, pair)
	}
	return kvpair
}

func Reduce(key string, words []string) string {
	//count the occurrence of the word with same length
	len := int64(len(words))
	//convert to string
	count := strconv.FormatInt(len, 10)
	return count
}