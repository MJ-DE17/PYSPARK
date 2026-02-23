import time
from traceback import print_tb

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSessionPractice") \
    .master("local[4]") \
    .config("spark.executor.memory" , "1g") \
    .config("spark.driver.memory" , "1g") \
    .getOrCreate()


print("Exec memory :" , spark.sparkContext.getConf().get("spark.executor.memory"))

df = spark.range(0 , 100000000)
start = time.time()
df.filter(df.id % 2 == 0).count()

end =time.time()
print("Local :" , spark.sparkContext.master)
print("Execution time : ", end - start)
spark.stop()