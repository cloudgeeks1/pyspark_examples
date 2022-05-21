# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.option("header","true").csv(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/naukri_data_science_jobs_india.csv")

df.show()
