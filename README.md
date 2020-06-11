- bin/zookeeper-server-start.sh config/zookeeper.properties

- bin/kafka-server-start.sh config/server.properties

- kafka-topics.sh --create --topic test-topic -zookeeper localhost:2181 --replication-factor 1 --partitions 4

- kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning

- kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

- kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
