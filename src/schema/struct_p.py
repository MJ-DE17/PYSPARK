from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Strucut scheme").getOrCreate()

data = [("manas" , 43),("Sudharsana" , 24) , ("Thar" , 23)]
schema = StructType([
                    StructField("name" , StringType() , False),
                    StructField("age" , IntegerType() , True)])
df = spark.createDataFrame(data , schema)
df.show()