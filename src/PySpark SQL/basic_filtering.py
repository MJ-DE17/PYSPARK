from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL IN PS").getOrCreate()

data = [
    (1, "Manasa", 23),
    (2, "Arun", 30),
    (3, "Kavi", 19),
    (4, "Riya", 27)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data ,columns)
df.createOrReplaceTempView("students")

result = spark.sql("""
        SELECT name FROM students
        WHERE age >25""")
result.show()

