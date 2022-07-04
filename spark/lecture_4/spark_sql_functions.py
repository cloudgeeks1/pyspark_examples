# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("cloudgeeks") \
    .getOrCreate()


df = spark.read.option("header","true").json(
    "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json")

print(df.select("petalLength").show())

print(df.select("petalLength").distinct().show())

print(df.select("petalLength").count())

print(df.select("petalLength").distinct().count())



#hower to the sample method and go through the information.
