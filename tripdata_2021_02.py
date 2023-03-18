import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, concat_ws, count

import pandas as pd
import urllib.request

# create spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('testing') \
    .getOrCreate()

# download the data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-02.parquet"
filename = "fhvhv_tripdata_2021-02"
urllib.request.urlretrieve(url,filename)

# convert parquet to csv
df = pd.read_parquet('fhvhv_tripdata_2021-02.parquet')
df.to_csv('fhvhv_tripdata_2021-02.csv')
# checking the null value
df.isnull().sum()

# null value to "Nan Value"
df["originating_base_num"] = df["originating_base_num"].fillna("Nan Values")
df["on_scene_datetime"] = df["on_scene_datetime"].fillna("Nan Values")
df["airport_fee"] = df["airport_fee"].fillna("Nan Values")
df["request_datetime"] = df["request_datetime"].fillna("Nan Values")

# pyspark read dataframe
df1 = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-02.csv')

df1.show
df1.printSchema()

# filtering
# Question 1
filter_condition = col('pickup_datetime').contains('2021-02-15')
df_15feb = df1.filter(filter_condition)
df_15feb_count = df_15feb.count()
print(f"How many taxi trips were there on February 15? There are: {df_15feb_count} trips")

# Question 2
df_max_trip_miles = df1.groupBy(date_format(col('pickup_datetime'), 'yyyy-MM-dd') \
                                .alias('pickup_date')).agg(max(col('trip_miles')) \
                                .alias('max_trip_miles'))
print('Find the longest trip for each day?')
df_max_trip_miles.show()

# Question 3
dispatch = df1.groupBy(col('dispatching_base_num')).count()
top5_dispatch = dispatch.orderBy(col('count').desc()).limit(5)
print('Find Top 5 Most frequent `dispatching_base_num`')
top5_dispatch.show()

# Question 4
df_with_pair = df1.withColumn('location_pair', concat_ws('->', col('PUlocationID'), col('DOlocationID')))
df_location_pair = df_with_pair.groupBy('location_pair').agg(count('*').alias('count'))
top5_location_pair = df_location_pair.orderBy(col('count').desc()).limit(5)
print('Find Top 5 Most common location pairs (PUlocationID and DOlocationID)')
top5_location_pair.show()