
TO PERFORM BASIC OPERATIONS ON RDD USING PYSPARK WEB JUPYTER :

Hadoop Version :(hadoop version):3.3.1

Python Version:(python3 --version):3.8.10

Spark Version :(spark-shell  --version):3.1.2


Start all hadoop services and pyspark :

           $ start-all.sh

           $ jps

         ~$ pyspark

Directs to web jupyter :

Parallelize() is a method to create an RDD from an existing collection (For e.g Array) present in the driver.

num = sc.parallelize(range(1,10))


collect()  is an action to retrieve the all elements

''' gets partitions depending on core'''

	num.getNumPartitions()

'''giving partitions '''
	
	num = sc.parallelize(range(1,10),8)

 '''use of repartition ---can be used for increasing and decreasing but preferably used for increasing; to increase to 10'''
	
	num2 = num.repartition(10)

'''to reduce no of partitions using coalesce from 10 to 5'''
	
	num3 = num2.coalesce(5)

'''to take the file locally print the data of the file using collect()'''

	file_obj = sc.textFile("file:///home/lenovo/textfile.txt")
	
	file_obj.collect()

'''to take the first two lines using take()'''
	
	file_obj.take(2)

'''to get the type '''

	type(file_obj.take(2))

'''use of count to get the no. of elements or rows '''
	
	file_obj.count()

'''to get first line'''
	
	file_obj.first()

'''To get flattened data using flatMap() transformation'''
	
	list_created = file_obj.flatMap(lambda line : line.split(" "))

map( ): The map() function iterates through all items in the given iterable and executes the function we passed as an argument on each of them.
           map(function, iterable(s))
Gives nested list

To stop jupyter ctrl+c  and  to stop  hadoop: 
       
       $ stop-all.sh 
       
       $jps
