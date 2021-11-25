PERFORM SQL QUERIES BY ACCESSING ALL CSV  FILES FROM HDFS :

Hadoop Version :(hadoop version):3.3.1

Python Version:(python3 --version):3.8.10

Spark Version :(spark-shell --version):3.1.2

Start the hadoop services:

~$ start-all.sh

  ~$ jps

Start pyspark in terminal :

   ~$ pyspark
   
Download all 6 csv files and make them into folder in local system 

Place all the csv files into hdfs inside a directory :

$ hadoop fs -mkdir /sparkallcsv

$ hadoop fs -put /home/lenovo/Downloads/cpu_csv/*.csv /sparkallcsv

Perform the SQL queries 

Applying plots for the queries output by using MATPLOTLIB and seaborn library 

Stop all the servers by :

      $stop-all.sh

       $jps 


