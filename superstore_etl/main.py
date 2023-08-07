from extract import extract
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('ETLPipeline') \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .getOrCreate()

sql = "select * from orders"
df = extract(spark=spark, sql=sql)
print(df.show(5))