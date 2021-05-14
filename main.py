import os
from pyspark.sql import SparkSession
from processing import extract_nested_fields, extract_nested_fields1 ,get_zone
from schema import get_schema_taxi_request, get_schema_ride


def run_stream():
    # Initialize the stream
    spark = SparkSession.builder.master("local").getOrCreate()

    # Subscribe to a kafka topic "taxi"
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
        .option("subscribe", "taxi") \
        .load()

    schema = get_schema_taxi_request()
    schema1 = get_schema_ride()

# Business logic
    df_with_unnested_fields = extract_nested_fields(df, schema)
    df_with_zone = get_zone(df_with_unnested_fields)

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df_with_zone \
        .writeStream \
        .format("console") \
        .start() \
        .awaitTermination()

if __name__ == '__main__':
    run_stream()