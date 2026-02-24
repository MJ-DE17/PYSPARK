# 1. Transformations (Lazy)
# map()
# filter()
# flatMap()
# distinct()
# union()
# 2. Actions (Trigger Execution)
# collect()
# count()
# first()
# take()
# reduce()
from traceback import print_tb

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Basic Operations").getOrCreate()
sc = spark.sparkContext

nums = [1,2,3,4,5,5,6]

rdd1 = sc.parallelize(nums)

# Step 2: Square using map

sq_rdd = rdd1.map(lambda x : x*x)
print(sq_rdd.collect())

# : Filter Even Numbers

even_rdd = rdd1.filter(lambda x : x % 2 == 0)
print(even_rdd.collect())

# word count
text = sc.textFile("file1.txt")
# rdd/file1.txt

words = text.flatMap(lambda line : line.split(" "))
# Step 3: Map each word to (word,1)
pairs = words.map(lambda word : (word, 1))
# Step 4: Count using reduceByKey
counts = pairs.reduceByKey(lambda a , b : a+b)
print(counts.collect())


# Problem 5: Remove Duplicates
rdd = sc.parallelize([1,2,2,3,3,3,4])
unique = rdd.distinct()
print(unique.collect())

# Problem 6: Count Words Starting With 's'
text = sc.textFile("file1.txt")
words = text.flatMap(lambda line: line.split(" "))
s_words = words.filter(lambda w: w.startswith("s"))
print(s_words.count())