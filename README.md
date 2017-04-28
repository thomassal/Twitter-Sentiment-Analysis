Twitter Sentiment Analysis
==========================

Sentiment analysis in Twitter data with Apache Spark (Spark-streaming, spak-sql, GraphX).
 
* retrieve tweets using Spark Streaming & twitter4j
* sentiment analysis (tweet level, hashtag level)
* Wang, Xiaolong, et al. "Topic sentiment analysis in twitter: a graph-based hashtag sentiment classification approach." 
  Proceedings of the 20th ACM international conference on Information and knowledge management. ACM, 2011.

Data
------------

The data can be downloaded at the following link:

    http://help.sentiment140.com/for-students
	

Build
------------

You can build the project with the following command:

    sbt package



Run
------------

You can run the application with the following command:

    ./bin/spark-submit   
	--class TwitterSentimentAnalysis   
	--master local[4]   
	--packages "org.apache.spark:spark-streaming-twitter_2.10:1.6.1","com.databricks:spark-csv_2.11:1.4.0","org.apache.lucene:lucene-analyzers-common:5.3.0"  
	/YOURDIR../scala/target/scala-2.11/twitter-sentiment-analysis_2.11-1.0.jar