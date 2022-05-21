# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/json_files/wikidata_type_dict.json")

# df.show()
print(df.count())