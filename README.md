# kafka-spark-stream
### Homebrew
```
Apache Kafka 
B1
bin/zookeeper-server-start.sh config/zookeeper.properties
```
```
B2
bin/kafka-server-start.sh config/server.properties
```
```
B3
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mytesttopic

bin/kafka-topics.sh --list --zookeeper localhost:2181

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mytesttopic123

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic mytesttopic
```
```
B4
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic mytesttopic --from-beginning
```


