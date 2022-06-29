# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df1 = spark.read.option("header","true").json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json",multiLine=True)

df2 = spark.read.option("header","true").csv(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/naukri_data_science_jobs_india.csv")

df1.printSchema()
print("total data in df is: ",df1.count())

df2.printSchema()
print("total data in df is: ",df2.count())

union_df = df1.unionByName(df2,allowMissingColumns=True)

union_df.printSchema()
print("union dataframe count: ",union_df.count())


#hower to the sample method and go through the information.
