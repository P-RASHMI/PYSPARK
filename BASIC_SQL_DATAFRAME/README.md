TO PERFORM SQL QUERIES DATAFRAMES BY TAKING CSV FILE FROM HDFS :

Hadoop Version :(hadoop version):3.3.1

Python Version:(python3 --version):3.8.10

Spark Version :(spark-shell  --version):3.1.2

start all hadoop services and pyspark :

           $ start-all.sh

           $ jps

         ~$ pyspark

Directs to web jupyter :


Spark SQL provides spark.read().csv("file_name") to read a file or directory of files in CSV format into Spark DataFrame.
Here to read titanic.csv file from csvspark directory from hdfs
	
	df = spark.read.csv("hdfs://localhost:9000/csvspark/titanic.csv",header=True)

To print schema in tree format:

   df.printSchema()

'''To get the columns'''
	
	df.columns

'''To get the statistics '''
	
	df.describe().show()

'''use of head to get first row'''
	
	df.head()

'''create new column with values of fare column'''
	
	df.withColumn('New Fare',df['fare']).show(2)

'''To rename the column fare with Fare_changed'''
	
	df.withColumnRenamed('fare','Fare_changed').show()

’’’ selects multiple columns of dataframe,generates new dataframe and show()-->shows contents of dataframe’’’

	df.select("age","class","who").show()

'''get the count, grouped by age '''
	
	df.groupBy("Age").count().show()

Creates temporary view of table on which sql query can be applied here table name is ‘Table’
	
	df.createOrReplaceTempView("Table")

To perform sql query on Table to select specified columns
	
	df_sql = spark.sql("select survived,pclass,sex,age from Table")

'''To select columns from table where age is greater than 14 '''
	
	df_sql = spark.sql("select pclass,age,sex from Table where age > 14")

To stop jupyter ctrl+c and to stop hadoop:

      $ stop-all.sh 

       $jps

