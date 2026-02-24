from pyspark.sql import SparkSession

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


# select

select_df = df.select("name" , "salary")
print(select_df.collect())
select_df.show()

select_dept = df.select("department")
# print(select_dept.collect())
select_dept.show()
