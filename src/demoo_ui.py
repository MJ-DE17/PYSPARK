
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\MANASADEVI J\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\MANASADEVI J\AppData\Local\Programs\Python\Python311\python.exe"
from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Simple_UI_Demo") \
    .getOrCreate()

sc = spark.sparkContext

# Create RDD with 4 partitions
rdd = sc.parallelize([(1,2), (1,3), (2,4), (2,5)], 4)

print("Partitions:", rdd.getNumPartitions())

# Narrow transformation
rdd1 = rdd.map(lambda x: (x[0], x[1] + 1))

# Wide transformation
rdd2 = rdd1.reduceByKey(lambda a, b: a + b)

# ACTION
print("Result:", rdd2.collect())

# Print Spark UI link
print("Spark UI:", sc.uiWebUrl)

input("Press Enter to stop...")
spark.stop()