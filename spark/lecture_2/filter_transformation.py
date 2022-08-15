# Import SparkSession
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.option("header","true").csv(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/naukri_data_science_jobs_india.csv")

# df.show()

filtered_df = df.filter(df.Job_Role.contains("Sr"))



#
# df.filter(col("Job_Role")== "Sr. Data Scientist").show(truncate=False)
# #
# df.filter((col("Job_Role")== "Sr. Data Scientist") & (col("Location")== "Bangalore/Bengaluru")).show(truncate=False)
# #
# df.filter((col("Job_Role")!= "Sr. Data Scientist") & (col("Location")!= "Bangalore/Bengaluru")).show(truncate=False)
#
#
# #multiple value filter in one condition
# Location = ["Bangalore/Bengaluru","Pune","Mumbai"]
# df.filter((col("Location").isin(Location) & (col("Company") == "IBM"))).show(truncate=False)

#like function
df.filter(col("Location").like("%Ko%")).show(truncate=False)