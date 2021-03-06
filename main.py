from pyspark.sql import SparkSession
from processing import extract_fields_demands, get_zone
from schema import get_schema_taxi_request
from properties import db_properties

def run_stream():
    # Initialize the stream
    spark = SparkSession.builder.master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Supprimer les logs

    # Subscribe to a kafka topic "taxi"
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "taxi_demands") \
        .load()

    schema = get_schema_taxi_request()

    df_with_unnested_fields = extract_fields_demands(df, schema)
    df_with_unnested_fields.printSchema()
    df_with_zone = get_zone(df_with_unnested_fields, "pickup")

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df_with_zone \
        .writeStream \
        .foreachBatch(write_df) \
        .start() \
        .awaitTermination()

def write_df(dataframe, id):

    dataframe.write.jdbc(url=db_properties['url'], table='demands', mode='append', properties=db_properties)

if __name__ == '__main__':
    run_stream()

