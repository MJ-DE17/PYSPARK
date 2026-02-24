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

# Rename department to dept.
# df.withColumnRenamed("department" , "dept").show()

# drop :
df.drop("age").show()
# method 1
df.drop("age", "salary").show()

# method 2
cols = ["age", "salary"]
df.drop(*cols).show()