# Filter Students Above Age 23

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = [("manas" , 43),("Sudharsana" , 24) , ("Thar" , 23)]
column = ["name" , "age"]

spark = SparkSession.builder \
    .appName("Filter students") \
    .getOrCreate()

df = spark.createDataFrame(data ,column )

df_filtered =df.filter(col("age") > 23)
df_filtered.show()

spark.stop()




