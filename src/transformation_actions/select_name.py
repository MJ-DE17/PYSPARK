# Select Only Names

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Select by name").getOrCreate()


data = [("manas" , 43),("Sudharsana" , 24) , ("Thar" , 23)]
column = ["name" , "age"]

df = spark.createDataFrame(data , column)

df_Select = (df.select("name"))
df_Select.show()
spark.stop()