import os

from pyspark.sql import SparkSession


# setup arguments
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12-3.1.1.jar'


spark = SparkSession.builder.master("local").getOrCreate()


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
  .option("subscribe", "topic1") \
  .load()

# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
ds = df \
 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
 .writeStream \
 .format("console") \
 .start()\
 .awaitTermination()
