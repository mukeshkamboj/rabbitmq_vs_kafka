Create the topic. Note. you should go inside the kafka container. 
```
docker exec -u root infra_kafka_1 kafka-topics --zookeeper zookeeper:2181 --topic first_topic --create --partitions 3 --replication-factor 1
docker exec -u root infra_kafka_1 kafka-topics --zookeeper zookeeper:2181 --topic second_topic --create --partitions 3 --replication-factor 1
```

List the topics
```
docker exec -u root infra_kafka_1 kafka-topics --zookeeper zookeeper:2181 --list
```

Describe the topic
```
docker exec -u root infra_kafka_1 kafka-topics --zookeeper zookeeper:2181 --topic thirdTopic --describe
```

To produce the message with kafka-console-producer. It will open the > prompt. To exit CTL ^C 
```
docker exec -u root -it infra_kafka_1 kafka-console-producer --broker-list kafka:29092 --topic first_topic
```

To consume the message with kafka-console-consumer. It will open the > prompt. To exit CTL ^C . If you want to read from the beginning then add --from-beginning at the end.
```
docker exec -u root -it infra_kafka_1 kafka-console-consumer --bootstrap-server kafka:29092 --topic first_topic
```

