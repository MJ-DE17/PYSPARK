from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("Spark UI Demo") \
    .master("local[2]") \
    .getOrCreate()

# Create sample data
data = [
    ("India", 100),
    ("USA", 200),
    ("India", 300),
    ("UK", 400),
    ("USA", 500)
]

columns = ["Country", "Sales"]

df = spark.createDataFrame(data, columns)

# Wide transformation (causes shuffle)
result = df.groupBy("Country").sum("Sales")

# Action (triggers job)
result.show()

print("Open http://localhost:4040 to see Spark UI")

time.sleep(300)  # keeps UI alive for 60 seconds