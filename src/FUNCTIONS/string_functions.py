from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("String funcitno").getOrCreate()

data = [
    (1, "Manasa", 50000.0, 25),
    (2, "Mevin", 60000.0, 28),
    (3, "Pavithra", 55000.0, 26),
    (4, "Menakha", 70000.0, 30),
    (5, "Arjun", 45000.0, 24)
]

columns = ["id" , "name" , "salary " , "age"]

df = spark.createDataFrame(data , columns)

df.select(col("name") , upper(col("name")).alias("uppercase")).show()