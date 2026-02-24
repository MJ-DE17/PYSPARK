# Method 1: Using parallelize()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDD HandsOn") \
    .getOrCreate()

sc = spark.sparkContext

rdd1 = sc.parallelize([1,2,3,4,5] , 3)
print(rdd1.collect())
# With Partitions shows the number of partitions used
print(rdd1.getNumPartitions())

# From Another RDD (Transforming)
rdd2 = rdd1.map(lambda x : x * 2)
print(rdd2.collect())


# From DataFrame file

# if true consider row as the col names
df = spark.read.csv("data.csv" , header=True)
rdd3 = df.rdd
