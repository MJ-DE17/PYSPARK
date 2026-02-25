from pyspark import SparkConf , SparkContext

conf = SparkConf() \
    .setAppName("") \
    .setMaster("local[2]") \
    .set("spark.executor.memory" , "1g")

sc = SparkContext(conf=conf)