# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.option("header","true").json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json")

rdd = df.select("sepalLength").rdd
print(rdd.collect())
pair_rdd = rdd.map(lambda x: (x,1))
print(pair_rdd.collect())
reducekey = pair_rdd.reduceByKey(lambda a,b : a+b)


for element in reducekey.collect():
      print(element)


#read this before recording
#https://www.hadoopinrealworld.co m/what-is-the-difference-between-groupbykey-and-reducebykey-in-spark/#:~:text=Both%20reduceByKey%20and%20groupByKey%20result,do%20a%20map%20side%20combine.

# go over databricks.ppt to get more understanding.
#/Users/pratikjoshi/Documents/MarketCast/youtube/mis/databricks.pdf

#go through the screenshot named reducebykey_over_groupbykey in the same folder /youtube/mis/
