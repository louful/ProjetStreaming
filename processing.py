from pyspark.sql.functions import from_json, col, when
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from zone import ZONE_1, ZONE_2, ZONE_3, ZONE_4, ZONE_5, ZONE_6, ZONE_7, ZONE_8, ZONE_9
from properties import db_properties

def extract_fields_demands(dataframe: DataFrame, schema):
    df_test = dataframe.select(from_json(col("value").cast("string"), schema).alias("value")).select(
        F.col('value').getItem('timestamp'),
        F.col('value').getItem('client_uuid'),
        F.col('value').getItem('request_dt'),
        F.col('value').getItem('latitude').alias("_latitude"),
        F.col('value').getItem('longitude').alias("_longitude")
    )
    return df_test

def extract_fields_taxi_ride(dataframe: DataFrame, schema1):
    df_test1 = dataframe.select(from_json(col("value").cast("string"), schema1).alias("value")).select(
        F.col('value').getItem('timestamp'),
        F.col('value').getItem('amount'),
        F.col('value').getItem('pickup_time'),
        F.col('value').getItem('dropoff_time'),
        F.col('value').getItem('pickup_latitude').alias("pickup_latitude"),
        F.col('value').getItem('pickup_longitude').alias("pickup_longitude"),
        F.col('value').getItem('dropoff_latitude').alias("dropoff_latitude"),
        F.col('value').getItem('dropoff_longitude').alias("dropoff_longitude"),
        F.col('value').getItem('tips')
    )
    return df_test1

def get_zone(dataframe: DataFrame, direction):

    dataframe_with_zone = dataframe.withColumn("zone",
                                               when((col(f"{direction}_latitude") < ZONE_1.latitude_1) & (col(f"{direction}_latitude") > ZONE_1.latitude_2) & (col(f"{direction}_longitude") > ZONE_1.longitude_1) & (col(f"{direction}_longitude") < ZONE_1.longitude_2), "ZONE1")
                                               .when((col(f"{direction}_latitude") < ZONE_2.latitude_1) & (col(f"{direction}_latitude") > ZONE_2.latitude_2) & (col(f"{direction}_longitude") > ZONE_2.longitude_1) & (col(f"{direction}_longitude") < ZONE_2.longitude_2), "ZONE2")
                                               .when((col(f"{direction}_latitude") < ZONE_3.latitude_1) & (col(f"{direction}_latitude") > ZONE_3.latitude_2) & (col(f"{direction}_longitude") > ZONE_3.longitude_1) & (col(f"{direction}_longitude") < ZONE_3.longitude_2), "ZONE3")
                                               .when((col(f"{direction}_latitude") < ZONE_4.latitude_1) & (col(f"{direction}_latitude") > ZONE_4.latitude_2) & (col(f"{direction}_longitude") > ZONE_4.longitude_1) & (col(f"{direction}_longitude") < ZONE_4.longitude_2), "ZONE4")
                                               .when((col(f"{direction}_latitude") < ZONE_5.latitude_1) & (col(f"{direction}_latitude") > ZONE_5.latitude_2) & (col(f"{direction}_longitude") > ZONE_5.longitude_1) & (col(f"{direction}_longitude") < ZONE_5.longitude_2), "ZONE5")
                                               .when((col(f"{direction}_latitude") < ZONE_6.latitude_1) & (col(f"{direction}_latitude") > ZONE_6.latitude_2) & (col(f"{direction}_longitude") > ZONE_6.longitude_1) & (col(f"{direction}_longitude") < ZONE_6.longitude_2), "ZONE6")
                                               .when((col(f"{direction}_latitude") < ZONE_7.latitude_1) & (col(f"{direction}_latitude") > ZONE_7.latitude_2) & (col(f"{direction}_longitude") > ZONE_7.longitude_1) & (col(f"{direction}_longitude") < ZONE_7.longitude_2), "ZONE7")
                                               .when((col(f"{direction}_latitude") < ZONE_8.latitude_1) & (col(f"{direction}_latitude") > ZONE_8.latitude_2) & (col(f"{direction}_longitude") > ZONE_8.longitude_1) & (col(f"{direction}_longitude") < ZONE_8.longitude_2), "ZONE8")
                                               .when((col(f"{direction}_latitude") < ZONE_9.latitude_1) & (col(f"{direction}_latitude") > ZONE_9.latitude_2) & (col(f"{direction}_longitude") > ZONE_9.longitude_1) & (col(f"{direction}_longitude") < ZONE_9.longitude_2), "ZONE9")
                                               )

    return dataframe_with_zone

def zone_demand(dataframe_with_zone):
    dataframe_with_demand = dataframe_with_zone.groupBy("zone").count()

    return dataframe_with_demand

def test(dataframe_with_zone):
    dataframe_test = dataframe_with_zone.groupBy("zone").count()\
        .max()

    return dataframe_test

def moy(dataframe_with_zone):
    dataframe_moy = dataframe_with_zone.groupBy("zone").mean("amount").alias("MeanAmount")

    return dataframe_moy

def tips(dataframe_with_zone):
    dataframe_tips = dataframe_with_zone.groupBy("zone").agg(sum("tips").alias("TotalTips"))

    return dataframe_tips

def sum(dataframe_with_zone):
    dataframe_sum = dataframe_with_zone.groupBy("zone").agg(sum("amount").alias("TotalAmount"))

    return dataframe_sum

