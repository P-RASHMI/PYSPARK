TWITTER  SENTIMENT ANALYSIS using PYSPARK ,tweepy for LIVE Twitter API To get positive negative count and PLOT GRAPH FOR THE COUNT,SENTIMENTS :

Hadoop Version :(hadoop version):3.3.1
Python Version:(python3 --version):3.8.10
Spark Version :(spark-shell --version):3.1.2

Install tweepy,textblob in terminal;
 
~$ pip3 install tweepy

~$ pip3 install textblob

Then store the consumer_key,consumer_secret,access_token and access_token_secret in .env file 

From .env file  fetch the details to access  real time data from twitter ,give logger.py file to get information about the events

Print the positive negative tweets count for the word ‘VIR DAS’ and store it to csv file.

Form that csv file we have to plot a real time graph using FuncAnimation library from matplotlib.animation

Here while running the tweets analysis you can also run the graph to get the live graph and analysize how data getting changed,being plotted as well.

