from pyspark.sql import SparkSession
from pyspark.sql.functions import sum , avg , count , max , min

spark = SparkSession.builder.appName("Group By Practice").getOrCreate()

data = [
    (1, "IT", 40000),
    (2, "HR", 35000),
    (3, "IT", 45000),
    (4, "Finance", 50000),
    (5, "HR", 30000),
    (6, "IT", 42000)
]

columns = ["id", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.show()

# Q1. Total salary per department
tot_salary_dept = df.groupBy("department").agg(sum("salary")).show()

# Q2. Average salary per department
avg_sal_dept = df.groupBy("department").agg(avg("salary")).show()

# Q3. Count employees per department
df.groupBy("department").agg(count("id")).show()

# Q4. Maximum salary per department
df.groupBy("department").agg(max("salary").alias("highest salary")).show()

# Q5. Multiple aggregations together
df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    count("id").alias("emp_count")
).show()

# multiple columns grouping
df2.groupBy("department", "gender") \
    .agg(sum("salary").alias("total_salary")) \
    .show()