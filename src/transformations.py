# ==========================================================
# PySpark Transformations and Actions Examples
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder.appName("Transformations_Actions_Demo").getOrCreate()

# Sample DataFrame
data = [
    (1, "Manasa", 25, "IT", 50000),
    (2, "Mevin", 30, "HR", 60000),
    (3, "Pavi", 22, "IT", 45000),
    (4, "Menakha", 28, "Finance", 70000)
]

columns = ["id", "name", "age", "department", "salary"]

df = spark.createDataFrame(data, columns)

# ==========================================================
# ------------------ NARROW TRANSFORMATIONS ----------------
# ==========================================================

# select()
df_select = df.select("name", "salary")

# filter()
df_filter = df.filter(col("age") > 25)

# where()
df_where = df.where("salary > 50000")

# withColumn()
df_withColumn = df.withColumn("bonus", col("salary") * 0.1)

# drop()
df_drop = df.drop("age")

# withColumnRenamed()
df_rename = df.withColumnRenamed("name", "employee_name")

# sample()
df_sample = df.sample(0.5)

# limit()
df_limit = df.limit(2)

# coalesce()
df_coalesce = df.coalesce(1)

# union()
df_union = df.union(df)

# ==========================================================
# ------------------- WIDE TRANSFORMATIONS -----------------
# ==========================================================

# groupBy()
df_group = df.groupBy("department")

# agg()
df_agg = df.groupBy("department").agg(sum("salary").alias("total_salary"))

# join()
df_join = df.join(df_select, on="name", how="inner")

# orderBy()
df_order = df.orderBy("salary")

# sort()
df_sort = df.sort("age")

# distinct()
df_distinct = df.select("department").distinct()

# dropDuplicates()
df_dropDuplicates = df.dropDuplicates(["department"])

# repartition()
df_repartition = df.repartition(2)

# rollup()
df_rollup = df.rollup("department").sum("salary")

# cube()
df_cube = df.cube("department").avg("salary")

# ==========================================================
# ------------------------- ACTIONS ------------------------
# ==========================================================

# show()
df.show()

# count()
row_count = df.count()

# collect()
all_rows = df.collect()

# first()
first_row = df.first()

# take()
top_two = df.take(2)

# write()
# df.write.csv("output_folder")   # Uncomment to write

# foreach()
df.foreach(lambda row: print(row))

# ==========================================================
# ---------------------- RDD EXAMPLES ----------------------
# ==========================================================

rdd = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3)])

# map()
rdd_map = rdd.map(lambda x: (x[0], x[1] * 2))

# flatMap()
rdd_flatMap = spark.sparkContext.parallelize(["hello world"]).flatMap(lambda x: x.split(" "))

# reduceByKey()
rdd_reduce = rdd.reduceByKey(lambda a, b: a + b)

# groupByKey()
rdd_group = rdd.groupByKey()

# sortByKey()
rdd_sort = rdd.sortByKey()

# reduce() (Action)
rdd_sum = spark.sparkContext.parallelize([1, 2, 3]).reduce(lambda a, b: a + b)

# collectAsMap() (Action)
rdd_map_collect = rdd_reduce.collectAsMap()

# saveAsTextFile() (Action)
# rdd.saveAsTextFile("rdd_output")   # Uncomment to save

# ==========================================================
# End of File
# ==========================================================

spark.stop()