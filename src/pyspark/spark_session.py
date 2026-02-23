from pyspark.sql import SparkSession
# create session
spark = (
    SparkSession
    .builder \
    .appName("Spark Intro") \
    .master("local[*]") \
    .getOrCreate()
)

# Inspect Configurations

print("Spark session created")
print("App Name :" ,spark.sparkContext.appName)
print("Master :" , spark.sparkContext.master )

print("\n All Congigurations: ")
for item in spark.sparkContext.getConf().getAll():
    print(item)
# # notes:
# SparkSession contains SparkContext
# Configurations are stored inside SparkConf
# .getConf().getAll() shows runtime configuration