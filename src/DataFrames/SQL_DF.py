from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Basic Operations").getOrCreate()
sc = spark.sparkContext

data = [(1, "Ram", 23),
        (2, "John", 30),
        (3, "Anu", 28)]

columns = ["id", "name", "age"]
df = spark.createDataFrame(data,columns)

# sql queries inside the df
df.createOrReplaceTempView("people")

result = spark.sql("SELECT * FROM people WHERE age > 25")
result.show()