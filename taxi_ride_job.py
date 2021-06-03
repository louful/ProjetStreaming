import os
from pyspark.sql import SparkSession
from processing import extract_fields_taxi_ride, get_zone
from schema import get_schema_ride
from properties import db_properties

def run_stream():
    # Initialize the stream
    spark = SparkSession.builder.master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") #Supprimer les logs

    # Subscribe to a kafka topic "taxi"
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
        .option("subscribe", "taxi_ride") \
        .load()

    schema1 = get_schema_ride()

# Business logic
    df_with_unnested_fields_taxi = extract_fields_taxi_ride(df, schema1)
    df_with_unnested_fields_taxi.printSchema()
    df_with_zone = get_zone(df_with_unnested_fields_taxi, "pickup")
    df_with_zone2 = get_zone(df_with_zone, "dropoff")

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df_with_zone2 \
        .writeStream \
        .foreachBatch(write_df) \
        .start() \
        .awaitTermination()

def write_df(dataframe, id):

    dataframe.write.jdbc(url=db_properties['url'], table='rides', mode='append', properties=db_properties)

if __name__ == '__main__':
    run_stream()
