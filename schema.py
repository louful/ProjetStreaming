from pyspark.sql.types import StructType, StructField, StringType, FloatType


def get_schema_taxi_request():
    schema = StructType(
        [
            StructField('timestamp', StringType(), True),
            StructField('client_uuid', StringType(), True),
            StructField('latitude', FloatType(), True),
            StructField('longitude', FloatType(), True)

        ]
    )
    return schema

def get_schema_ride():
    schema1 = StructType(
        [
            StructField('timestamp', StringType(), True),
            StructField('amount', FloatType(), True),
            StructField('pickup_latitude', FloatType(), True),
            StructField('pickup_longitude', FloatType(), True),
            StructField('dropoff_latitude', FloatType(), True),
            StructField('dropoff_longitude', FloatType(), True),
            StructField('tips', FloatType(), True)

        ]
    )
    return schema1
