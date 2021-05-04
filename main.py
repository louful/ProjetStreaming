import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F



# setup arguments
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12-3.1.1.jar'


spark = SparkSession.builder.master("local").getOrCreate()

schema = StructType(
    [
        StructField('key', StringType(), True),
        StructField('client_uuid', StringType(), True),
        StructField('request_dt', StringType(), True),
        StructField('latitude', StringType(), True),
        StructField('longitude', StringType(), True)

    ]
)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
  .option("subscribe", "taxi") \
  .load()




df_test = df.select(from_json(col("value").cast("string"), schema).alias("value"))\
    .select(
    F.col('value').getItem('key'),
    F.col('value').getItem('client_uuid'),
    F.col('value').getItem('request_dt'),
    F.col('value').getItem('latitude'),
    F.col('value').getItem('longitude'))

# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
ds = df_test \
 .writeStream \
 .format("console") \
 .start()\
 .awaitTermination()
