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

# filter hr , age < 30
filter_hr = df.filter((col("department") == "HR") & (col("age") < 30))
filter_hr.show()

# Show employees with salary greater than 40000
df.filter(col("salary") > 40000).show()

# DataFrame conditions:
# AND → &
# OR → |
# NOT → ~