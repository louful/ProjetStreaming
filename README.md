# Project Kafka SparkStreaming

## Set up Kafka single broker 

Start Zookepeer, Kafka controler, Kafka Brooker 
```
docker-compose up -d
```
Stop all 
```
docker-compose down
```
Check all containers ( -a for all  even those who are in use)

```
docker ps -a
```
## Start Spark Job


```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --py-files main.py main.py  
```
--packages for depence kafka

##Start Kafka in the terminal 

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-console-producer.sh --topic taxi --bootstrap-server localhost:9092
