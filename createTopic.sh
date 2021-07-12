#!/bin/bash -x
# createTopic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
# list all topic
kafka-topics --list --zookeeper localhost:2181


# consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic guardian2

# producer
kafka-console-producer --broker-list localhost:9092 --topic guardian2