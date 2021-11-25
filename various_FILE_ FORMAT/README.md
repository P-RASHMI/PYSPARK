READING AND WRITING OF DIFFERENT FILE FORMATS FROM HDFS:

Hadoop Version :(hadoop version):3.3.1

Python Version:(python3 --version):3.8.10

Spark Version :(spark-shell --version):3.1.2

In terminal install findspark: $pip3 install findspark 

Create directory in hadoop : $hadoop fs -mkdir /csvspark

upload titanic.csv in that directory using put command

Reading csv file from hdfs :df=spark.read.csv("hdfs file path",header=True)

	df.show()-->to get the data

write into csv file in hdfs:df.write.option("header","true").csv("hdfs file path")

Reading json file from hdfs :df1=spark.read.json("hdfs file path",multiLine=True)

write into json file : df1.write.json("hdfs file path")

Reading csv and converting into parquet file by writing into parquet file:

	df.write.parquet("hdfs parquet file path")
	
Reading from parquet file:dataframe_par = spark.read.parquet("hdfs parquet file path")

Reading csv and converting into AVRO FILE by writing into AVRO file:

In another terminal start spark shell with packages and in scala implement reading and writing:

	~$ spark-shell --packages org.apache.spark:spark-avro_2.12:2.4.5
	
In the shell terminal read the csv file from hdfs as convert to avro file and store in hdfs

val data_csv = spark.read.csv("hdfs://localhost:9000/csvspark/titanic.csv")

Convert  the dataframe of csv into avro file and write it into hdfs :

	 dataframe.write.format(“avro”).save(“path of hdfs file”)

	Just give path file and (create directory but dont create file)
	
write into avro file: df.write.format("avro").save("hdfs avro file path")
	
	scala> data_csv.write.format("avro").save("hdfs://localhost:9000/jsonread/writeto.avro")
	
Read from avro file : val df =spark.read.format("avro").load("hdfs avro file path")

	scala> val data_avro = spark.read.format("avro").load("hdfs://localhost:9000/jsonread/writeto.avro")

close the spark shell with ctrl+c and stop hadoop servers--->

	stop-all.sh
        jps 


	





