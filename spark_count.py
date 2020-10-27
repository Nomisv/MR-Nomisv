from pyspark import SparkConf, SparkContext
import string
import re


def preprocess(line):

 line = line.lower()
 line = line.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
 line = line.translate(str.maketrans('—', ' ' * len('—')))
 line = line.split()

 return line
 
 
 
 
def line_to_words(line): 
 regex = re.compile('[^a-zA-Z0-9]+')
 words = re.split(regex, line) # print(words) 
 return words
  
  
  
conf = SparkConf().setAppName("wordcount").setMaster("local[2]")
sc = SparkContext(conf=conf)

inputdata = sc.textFile("hamlet.txt")


output = inputdata.flatMap(lambda x: line_to_words(x)).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)


result = output.sortByKey().collect()
result = result[1:]

with open('testMR.txt', 'w') as f:
 for word_count in result:
  f.write(str(word_count[0]) + ' ' + str(word_count[1]) + '\n')
f.close()

sc.stop()	



