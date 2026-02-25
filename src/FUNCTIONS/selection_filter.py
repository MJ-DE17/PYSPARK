from Tools.demo.sortvisu import distinct
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
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
df.cache()
df.select(col("name ") , col("age")).show()

df.filter(col("salary") > 55000).show()

df.filter((col("salary") > 50000) & (col("age") < 30)).show()
# salary > 50000 AND age < 30

df.filter(~(col("age") < 28)).show()
# NOT age < 28

df.filter(col("age").between(20 , 30)).show()
# between

df.filter(col("name").isin("Manasa","Arjun")).show()
# isin (in)

df.filter(col("name").like('%M')).show()
# like

df.filter(col("salary").isNull()).show()
# salary is NULL

df.filter(col("salary").isNotNull()).show()
# salary not NULL

df.select(col("age")).distinct().show()
# unique ages

df.orderBy("salary").show()
# sorted ascending

df.orderBy(col("salary").desc()).show()
# sorted descending

df.filter(col("name").rlike("^M")).show()
# Regular Expressions

# starts with:
df.filter(col("name").rlike("^M")).show()

# ends with :
df.filter(col("name").rlike("a$")).show()

# contains :
df.filter(col("name").rlike("^[A-Z]")).show()


