from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Tables").enableHiveSupport().getOrCreate()


data = [
    (1, "Ravi", "Chennai"),
    (2, "Anu", "Salem")
]

columns = ["id", "name", "city"]

df = spark.createDataFrame(data, columns)

df.write.mode("overwrite").saveAsTable("emp_managed")
