# Import SparkSession
from pyspark.sql import SparkSession,functions as f
from logging import getLogger

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json")

df.show()
print("get count: ")
print(df.count())
