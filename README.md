# Pyspark-based-Stream-Data-Classification-System
Implement a system for real-time news stream classification, trend analysis and viasualization.

-	Fetched the feature by TF-IDF and Modeled by Naïve Byes with pyspark Pipeline
-	Generated the Kafka streaming data from Guardian API
-	Deployed the Elasticsearch and Kibana for data collection and representation

To useing this model you need to install following thing
1. kafka
2. zookeeper
3. python3
4. python2
5. elasticsearch
6. kibana

and the following library for python2 and python3
1. pyspark
2. elasticsearch

After finishing installation, you can start to run this project by belowing steps:

1. open zookeeper
	zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

2. open kafak
	kafka-server-start /usr/local/etc/kafka/server.properties

	2.1 if you need to create topic
		kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic guardian2

3. open consumer(to check you get the data from producer.if you don't want to check, you can skip this)
	kafka-console-consumer --bootstrap-server localhost:9092 --topic guardian2

4. run elasticsearch
	elasticsearch

5. run kibana
	kibana

6. pretrain model, remember to put train_data.txt and pretrain.py in the same folder. Only need to run one time
	python3 pretrain.py

7. run producer
	python3 stream_producer.py [API key] 2018-11-3 2018-12-24

8. predict model
	spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 PathToFile/classifier.py localhost:9092 guardian2

9. go to kibana check your result
	localhost:5601



sparkML.py: introduce how spark.ml using pipeline with 
- ETL: StopWordsRemover, NGram
- feature: TF-IDF, word2Vec, DCT, polyExpansion, PCA
- ML model: LogisticRegression, NaiveBayes, LinearSVC
