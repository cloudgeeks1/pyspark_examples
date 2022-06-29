from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('ES_indexer')\
    .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-hadoop:7.8.1')\
    .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:5.3.1')\
    .getOrCreate()

df = spark.read.json('/Users/pratikjoshi/PycharmProjects/cloudgeeks_pyspark_examples/data/wikidata_type_dict.json')

index_name = "my-index-000002"
doc_type_name = "doc"
df.write.format('org.elasticsearch.spark.sql')\
    .option('es.nodes.discovery', 'false')\
    .option('es.nodes', 'https://search-rp-rta-dev-bo6fjjyiodpgvjx5urkggd3xgi.us-east-1.es.amazonaws.com')\
    .option('es.port', 9200)\
    .option('es.nodes.resolve.hostname', 'false')\
    .option('spark.es.index.auto.create', 'true')\
    .option('es.nodes.cloud.only', 'false')\
    .option('es.net.ssl','false')\
    .option('es.resource', f'{index_name}/{doc_type_name}').save()