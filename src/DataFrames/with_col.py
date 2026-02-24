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


# Add column tax = salary * 0.10.
add_col = df.withColumn("tax" , col("salary") * 0.10).show()

# Add a new column salary_bonus = salary + 5000.

salary_bonus_col = df.withColumn("salary_bonus " , col("salary") + 5000).show()

# Add column age_after_10_years = age + 10.
pres_age = df.withColumn("Present_age" , col("age") + 10).show()