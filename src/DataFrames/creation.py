from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Basic Operations").getOrCreate()
sc = spark.sparkContext


# 1.From CSV
df = spark.read.csv("file1.csv" , header= True , inferSchema= True)
df.show()
# header=True → first row is column names
# inferSchema=True → Spark detects data types

# 2. From Python List
data = [(1, "Ram", 23), (2, "John", 30)]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

# 3.rdd to df
rdd = sc.parallelize([(1, "Ram", 23), (2, "John", 30)])
df = rdd.toDF(["id" , "name" , "age"])
df.show()
