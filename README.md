# mapreduce-Nomisv
mapreduce-Nomisv created by GitHub Classroom

from pyspark import SparkContext

import re

def line_to_words(line):
	regex = r'\w{1,}'
	words = re.findall(regex, line.lower())
	# print(words)
	return words

sc = SparkContext("local", "word count")
text_file = sc.textFile("hamlet.txt")
word_counts = text_file.flatMap(lambda line: line_to_words(line)).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)


with open('hamletout.txt', 'w') as f:
	for word_count in word_counts.collect():
		line = ' '.join(str(x) for x in word_count)
		f.write(line + '\n')
f.close()


