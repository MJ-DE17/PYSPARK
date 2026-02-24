
# Add New Column (Bonus) 10%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Add Bonus").getOrCreate()

data = [("manas" , 43 , 250000),("Sudharsana" , 24 , 550000) , ("Thar" , 23 , 900000)]
column = ["name" , "age" , "salary"]

df = spark.createDataFrame(data , column)
df_bonus = df.withColumn("bonus" , col("salary") *0.1 )

df_bonus.show()
spark.stop()