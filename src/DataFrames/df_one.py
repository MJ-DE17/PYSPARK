from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Dataframe eg") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("Manasa", 23),
    ("Alfie", 26),
    ("Pavithra", 21)
]

df = spark.createDataFrame(data , ["Name" , "Age"])

df.show()
df.filter(df.Age > 22).show()