from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL IN PS").getOrCreate()

data = [
    (1, "Chennai", 5000),
    (2, "Madurai", 7000),
    (3, "Chennai", 3000),
    (4, "Madurai", 2000),
    (5, "Trichy", 4000)
]

columns = ["order_id", "city", "amount"]

df = spark.createDataFrame(data ,columns)
df.createOrReplaceTempView("orders")

result = spark.sql("""select city , sum(amount) as "total amount " FROM  orders group by city having sum(amount) > 5000""")
result.show()

