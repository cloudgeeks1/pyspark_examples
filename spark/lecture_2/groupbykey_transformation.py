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
pair_rdd = rdd.map(lambda x: (x,1))

sum_per_key = pair_rdd.groupByKey().mapValues(list).collect()
#
print(sum_per_key)




#read this before recording
#https://www.hadoopinrealworld.com/what-is-the-difference-between-groupbykey-and-reducebykey-in-spark/#:~:text=Both%20reduceByKey%20and%20groupByKey%20result,do%20a%20map%20side%20combine.
