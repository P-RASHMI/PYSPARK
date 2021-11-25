'''
@Author: Rashmi
@Date: 2021-11-24 13:10
@Last Modified by: Rashmi
@Last Modified time: 2021-11-21  14:11
@Title : Perform wordcount using spark streaming in python 
by running in local servers
'''
import findspark
findspark.init()
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    
    # Create a local StreamingContext with two working thread and 
    # batch interval of 10 second
    sc = SparkContext("local[2]",appName="wordcountstreaming")
    ssc = StreamingContext(sc, 10)
    # Create a DStream that will connect to hostname:port,
    #  like localhost:9999
    lines = ssc.socketTextStream("localhost",9999)
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    #print words from generated RDD to console              
    counts.pprint()
    #start for the computation
    ssc.start()
    ssc.awaitTermination()  #wait for the computation
