# Import SparkSession
from pyspark.sql import SparkSession

from pyspark.sql import functions as func

from pyspark.sql.types import LongType

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.parquet(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/smaple.parquet.snappy")

df = df.withColumn('id_raw', func.col('id_raw').cast(LongType()))

df.printSchema()
print(df.count())
