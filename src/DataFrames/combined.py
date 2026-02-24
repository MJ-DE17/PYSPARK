from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Basic Operations").getOrCreate()

data = [
    (1, "Ram", 23, "IT", 40000),
    (2, "John", 30, "HR", 35000),
    (3, "Anu", 28, "IT", 45000),
    (4, "Meena", 35, "Finance", 50000),
    (5, "Karthik", 26, "HR", 30000)
]

columns = ["id", "name", "age", "department", "salary"]

df = spark.createDataFrame(data, columns)
# df.show()

# Show only name and salary for employees in IT department.
ans = df.filter(col("department") == "IT").select("name" , "salary").show()

# Add bonus column, then show only name and new bonus.
bonus_add = df.withColumn("bonus" , col("salary") + 10000 ).select("name" , "bonus").show()

# Filter employees with salary > 35000, then drop department column.
filter_salary = df.filter(col("salary") > 35000)
filter_salary.drop("department").show()