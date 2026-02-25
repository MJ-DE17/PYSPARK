from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DataViewingPractice") \
    .getOrCreate()

data = [
    (1, "Manasa", 50000.0, 25),
    (2, "Mevin", 60000.0, 28),
    (3, "Pavithra", 55000.0, 26),
    (4, "Menakha", 70000.0, 30),
    (5, "Arjun", 45000.0, 24)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# Default 20 rows (truncated) → | id | name | salary |
df.show(10)  
# First 10 rows → table format

df.show(5, False)  
# No truncation → full string visible

df.show(5, True, True)  
# Vertical view → -RECORD 0- id:1 name:Manasa salary:50000

df.show(truncate=30)
# String cut at 30 characters

df.show(vertical=True)  
# One record per block (good for wide tables)

# SCHEMA & STRUCTURE


df.printSchema()
# Tree format → root |-- id:int |-- name:string

# we have toprint upcoming three to see output

df.schema
# StructType([StructField('id', IntegerType(), True)])
df.columns
# ['id', 'name', 'salary']
df.dtypes
# [('id','int'), ('name','string')]

# 🔹 ROW ACCESS


df.first()
# First row → Row(id=1, name='Manasa')
df.head()
# Same as first() → Row(...)
df.head(3)
# List of 3 rows → [Row(...), Row(...)]
df.take(3)
# List of first 3 rows → [Row(...)]
df.collect()
# ALL rows to driver → [Row(...), Row(...)]

# 🔹 COUNT

df.count()
# Total row count → 1000


# 🔹 STATISTICS
df.describe().show()
# count, mean, stddev, min, max