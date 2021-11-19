TO PERFORM WORD COUNT BY TAKING TEXT FILE FROM LOCAL SYSTEM AND FROM HADOOP:

TO PERFORM WORD COUNT BY TAKING TEXT FILE FROM LOCAL SYSTEM :

To start the hadoop servers:
         ~$ start-all.sh
           ~$ jps

start the jupyter by using pyspark:    ~$ pyspark

To give the path of the file on which you want to perform word count:
            textfile = sc.textFile("file:///home/lenovo/textfile.txt")

To use flatMap to flatten the list of data and lambda function to split words with spaces:
            words = textfile.flatMap(lambda line: line.split(" ")) 

To use map function to assign each word with value 1 (key:value) and reduceByKey to increase the count for repetition of word:

wordcount = words.map(lambda word : (word,1)).reduceByKey(lambda a,b:a+b)

collect () to perform action for the transformations to get output:

wordcount.collect()

To stop web jupyter note book by ctrl+c in terminal and stop all services by 
   $stop-all.sh
   $jps


TO PERFORM WORD COUNT BY TAKING TEXT FILE FROM HADOOP :

start the hadoop servers:
         ~$ start-all.sh
           ~$ jps

Start the jupyter by using pyspark:    ~$ pyspark

Create new directory in terminal and put the text file from local system into hdfs :

$hadoop fs -mkdir /word_spark
$hadoop fs -put textfile.txt   /word_spark

To give the path of the file on which you want to perform word count:
    textfile = sc.textFile("hdfs://localhost:9000/word_spark/textfile.txt")

To use flatMap to flatten the list of data and lambda function to split words with spaces:
            words = textfile.flatMap(lambda line: line.split(" ")) 

To use map function to assign each word with value 1 (key:value) and reduceByKey to increase the count for repetition of word:

 wordcount = words.map(lambda word : (word,1)).reduceByKey(lambda a,b:a+b)

collect () to perform action for the transformations to get output:
      wordcount.collect()

To stop web jupyter note book by ctrl+c in terminal and stop all services by 

   $stop-all.sh
    $jps

