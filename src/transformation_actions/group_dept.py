# Count how many employees are in each department.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example5").getOrCreate()

data = [
    ("Manasa", "IT"),
    ("Mevin", "HR"),
    ("Pavi", "IT")
]

columns = ["name", "department"]

df = spark.createDataFrame(data, columns)

groupred_df = df.groupBy("department").count()

groupred_df.show()
spark.stop()