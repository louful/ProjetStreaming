# Project Kafka SparkStreaming


## To lunch the code on your computer you just have to :

```
git clone https://github.com/louful/ProjetStreaming.git
```

Lunch the virtual environment
```
virtualenv env
source env/bin/activate
```
Download all the librairies
```
pip3 install -r requirements.txt
```

## To lunch Docker

Start Zookepeer, Kafka controler, Kafka Brooker 
```
docker-compose up -d
```
Start Métabase 
```
docker run -d -p 3000:3000 --name metabase metabase/metabase
```
Stop all 
```
docker-compose down
```
Check all containers ( -a for all  even those who are in use)

```
docker ps -a
```
## Start Job

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --driver-class-path postgresql-42.2.20.jar --py-files  main.py main.py```
```

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --driver-class-path postgresql-42.2.20.jar --py-files  taxi_ride_job.py taxi_ride_job.py
```
