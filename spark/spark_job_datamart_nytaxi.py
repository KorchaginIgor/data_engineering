from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])

dim_columns = ['id', 'name']

payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]


def agg_calc(spark):
    data_path = '/user/root/2020/yellow_tripdata_2020-01.csv'

    trip_fact = spark.read \
        .option('header', 'true') \
        .format('csv') \
        .schema(trips_schema) \
        .load(data_path)

    datamart = trip_fact \
        .where((f.month(trip_fact['tpep_pickup_datetime']) == '1') & \
               (f.year(trip_fact['tpep_pickup_datetime']) == '2020')) \
        .groupby(trip_fact['payment_type'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('date')) \
        .agg(f.avg(trip_fact['total_amount']).alias('average_trip_cost'),
             (f.sum(trip_fact['total_amount']) / f.sum(trip_fact['trip_distance'])).alias('avg_trip_km_cost')) \
        .select(f.col('payment_type'),
                f.col('date'),
                f.col('average_trip_cost'),
                f.col('avg_trip_km_cost')) \

    return datamart


def create_dict(spark: SparkSession, header: list, data: list):
    """создание словаря"""
    df = spark.createDataFrame(data=data, schema=header)
    return df


def main(spark: SparkSession):
    payment_dim = create_dict(spark, dim_columns, payment_rows)
    datamart = agg_calc(spark).cache()

    joined_datamart = datamart \
        .join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
        .select(f.col('name').alias('Payment type'),
                f.col('date').alias('Date'),
                f.round(datamart['average_trip_cost'], 2).alias('Average trip cost'),
                f.round(datamart['avg_trip_km_cost'], 2).alias('Avg trip km cost')) \
        .orderBy(f.col('date').desc(), f.col('name'))

    #    datamart.show()
    joined_datamart.show()
    joined_datamart \
        .repartition(1) \
        .write.format("csv") \
        .option('header', 'true') \
        .save('/user/ikorchagin/spark_hw1_output2/')


if __name__ == '__main__':
    main(SparkSession
         .builder
         .appName('My first Spark Job')
         .getOrCreate())
