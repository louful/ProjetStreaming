from pyspark.sql.functions import from_json, col, when
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from zone import ZONE_1, ZONE_2, ZONE_3, ZONE_4, ZONE_5, ZONE_6, ZONE_7, ZONE_8, ZONE_9

def extract_fields_demands(dataframe: DataFrame, schema):
    df_test = dataframe.select(from_json(col("value").cast("string"), schema).alias("value")).select(
        F.col('value').getItem('key'),
        F.col('value').getItem('client_uuid'),
        F.col('value').getItem('request_dt'),
        F.col('value').getItem('latitude').alias("latitude"),

        F.col('value').getItem('longitude')
    )
    return df_test

def extract_fields_taxi_ride(dataframe: DataFrame, schema1):
    df_test1 = dataframe.select(from_json(col("value").cast("string"), schema1).alias("value")).select(
        F.col('value').getItem('key'),
        F.col('value').getItem('amount'),
        F.col('value').getItem('pickup_time'),
        F.col('value').getItem('dropoff_time'),
        F.col('value').getItem('pickup_latitude'),
        F.col('value').getItem('pickup_longitude'),
        F.col('value').getItem('dropoff_latitude'),
        F.col('value').getItem('dropoff_longitude'),
        F.col('value').getItem('tips')
    )
    return df_test1

def get_zone(dataframe: DataFrame):

    dataframe_with_zone = dataframe.withColumn("zone",
                                               when((col("latitude") < ZONE_1.latitude_1), "ZONE1" )

                                            )

    return dataframe_with_zone
