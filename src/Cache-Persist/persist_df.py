from pyspark.sql import SparkSession
from pyspark import StorageLevel



spark = SparkSession.builder.appName("Learn Chache Persist").getOrCreate()

df = spark.range(1, 10000000)

even_df = df.filter("id % 2 = 0")
even_df.persist(StorageLevel.MEMORY_AND_DISK)

print(even_df.count())
even_df.show()