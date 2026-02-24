from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Learn Chache Persist").getOrCreate()

df = spark.range(1, 10000000)

even_Df = df.filter("id % 2 = 0")

even_Df.cache()
even_Df.count()
even_Df.collect()

# Check if DataFrame is Cached
even_Df.is_cached       # True or False

# removes cache
even_Df.unpersist()
