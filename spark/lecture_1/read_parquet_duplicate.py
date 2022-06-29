# Import SparkSession
import pandas
from pyspark.sql import SparkSession

from pyspark.sql import functions as func
from pyspark.sql.functions import col

from pyspark.sql.types import LongType, IntegerType

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()



df = pandas.read_parquet("/Users/pratikjoshi/Downloads/activity_data2022-03-11_183436.0215881.parquet.snappy")

print(df.info(verbose=True))

print(len(df.index))

# 1000714

