# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("cloudgeeks") \
      .getOrCreate()


df = spark.read.option("header","true").json(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json")

df.printSchema()

#changing the data type of column
new_df = df.withColumn("petalLength",col("petalLength").cast("string"))
new_df.printSchema()

#adding new column
new_col_df = new_df.withColumn("Salary",col("petalWidth")*2)
new_col_df.show()

#adding constant value using lit function
state_column = new_col_df.withColumn("Country", lit("India")) \
  .withColumn("State",lit("Maharastra"))
state_column.show()

#renaming the column
state_column.withColumnRenamed("Country","Country_Name").show()

#drop column
state_column.drop("State").show()
