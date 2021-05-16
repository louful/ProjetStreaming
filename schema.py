from pyspark.sql.types import StructType, StructField, StringType ,FloatType


def get_schema_taxi_request():
    schema = StructType(
        [
            StructField('timestamp', StringType(), True),
            StructField('client_uuid', StringType(), True),
            StructField('request_dt', StringType(), True),
            StructField('latitude', FloatType(), True),
            StructField('longitude', FloatType(), True)

        ]
    )
    return schema

def get_schema_ride():
    schema1 = StructType(
        [
            StructField('timestamp', StringType(), True),
            StructField('amount', StringType(), True),
            StructField('pickup_time', StringType(), True),
            StructField('dropoff_time', StringType(), True),
            StructField('pickup_latitude', StringType(), True),
            StructField('pickup_longitude', StringType(), True),
            StructField('dropoff_latitude', StringType(), True),
            StructField('dropoff_longitude', StringType(), True),
            StructField('tips', StringType(), True)

        ]
    )
    return schema1
