#!/bin/bash -x
# 1. startServer : zookeeper -> kafka
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
kafka-server-start /usr/local/etc/kafka/server.properties
# 2. kafka consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic guardian2
# 3. kafka stream producer
python3 stream_producer.py e78b1eed-79b7-4a7d-959f-a79b5514245b 2018-11-3 2018-12-24
# 4. elasticsearch localhost:9200
elasticsearch
# 5. kibana localhost:5054
kibana
# 6. run classifier.py by belowing commend
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 Desktop/big\ data/hw3/classifier.py localhost:9092 guardian2
