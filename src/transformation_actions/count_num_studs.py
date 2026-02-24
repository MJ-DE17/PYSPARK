# Count total number of students.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = [("manas" , 43),("Sudharsana" , 24) , ("Thar" , 23)]
column = ["name" , "age"]

spark = SparkSession.builder \
    .appName("Filter students") \
    .getOrCreate()
df = spark.createDataFrame(data , column)

total = df.count()

print("Total nnum of students :" , total)
spark.stop()