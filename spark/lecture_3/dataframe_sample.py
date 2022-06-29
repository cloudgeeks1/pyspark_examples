# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.option("header","true").json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json")

df.printSchema()
print("total data in df is: ",df.count())
new_df = df.sample(False,0.8)
print("sample data count is: ",new_df.count())

#hower to the sample method and go through the information.
