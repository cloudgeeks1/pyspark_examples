from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CloudGeeks').getOrCreate()

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]

df = spark.read.option("header","true").csv(
      "/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/naukri_data_science_jobs_india.csv")

for element in df.rdd.collect():
    print(element)


flatmap_df = df.rdd.flatMap(lambda x: x)

for element in flatmap_df.collect():
    print(element)



# filtered_df = df.filter((df.Company == "SNAPMINDS TECHNOLOGIES LLP") & (df.Job_Role == "Data Scientist - MLB" ))
#
# filtered_df.show()
#
# flatmap = filtered_df.rdd.flatMap(lambda x: x)
# for element in flatmap.collect():
#     print(element)

