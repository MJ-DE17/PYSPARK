from pyspark.sql import SparkSession

# import os
# os.environ["SPARK_LOCAL_DIRS"] = "C:\\spark_temp"

# Create Spark Session
spark = SparkSession.builder \
    .appName("Spark UI Demo") \
    .master("local[2]") \
    .getOrCreate()

# Create Sample Data
data = [("India", 100),
        ("USA", 200),
        ("India", 300),
        ("UK", 400),
        ("USA", 500)]

columns = ["Country", "Sales"]

df = spark.createDataFrame(data, columns)

# Wide Transformation
grouped = df.groupBy("Country").sum("Sales")

# Action
grouped.show()

input("Press Enter to exit")  # Keeps Spark UI alive
