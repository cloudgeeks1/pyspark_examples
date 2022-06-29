# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df1 = spark.read.option("header","true").json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json")

df2 = spark.read.option("header","true").json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict1.json")

df1.printSchema()
print("total data in df is: ",df1.count())

df2.printSchema()
print("total data in df is: ",df2.count())

union_df = df1.union(df2)

#unionall is deprecated since 2.0 and converted to union only.
#hower to the union method and go through the information.

union_df.printSchema()
print("union dataframe count: ",union_df.count())



