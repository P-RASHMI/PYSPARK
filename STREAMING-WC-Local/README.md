WORDCOUNT PROGRAM USING  PYSPARK STREAMING RUNNING AT LOCAL SERVERS: 

Hadoop Version :(hadoop version):3.3.1

Python Version:(python3 --version):3.8.10

Spark Version :(spark-shell --version):3.1.2

In vs code give the python code with local and port as 9999

Import packages and findspark

# Create a local StreamingContext with two working thread and batch interval of 10  second
	
	 ssc = StreamingContext(sc, 10)

	sc = SparkContext(appName="PythonStreamingNetworkWordCount")

# Create a DStream that will connect to hostname:port, like localhost:9999

	lines = ssc.socketTextStream("localhost", 9999)

# Print words from generated RDD to console

	wordCounts.pprint()

	ssc.start()             # Start the computation

	ssc.awaitTermination()  # Wait for the computation to terminate

In the terminal give the command to run Netcat (a small utility found in most Unix-like systems) as a data server by using:

	~$ nc -lk 9999

Write the lines in this terminal  to get word count in vs code terminal
 
Go to vs code run the streamingsparkwc.py  and we can see wordcount in the terminal;






