schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# ------------------------------------------------------------
# 3. Create DataFrame
# ------------------------------------------------------------

data = [
    (1, "Alice", "HR", 50000),
    (2, "Bob", "IT", 60000),
    (3, "Charlie", "IT", 70000),
    (4, "David", "Finance", 55000),
    (5, "Eva", "HR", 65000)
]

df = spark.createDataFrame(data, schema)

print("Initial Data:")
df.show()

# ============================================================
# BASIC TRANSFORMATIONS
# ============================================================

print("Select:")
df.select(col("name"), "salary").show()

print("Filter:")
df.filter(col("salary") > 60000).show()

print("WithColumn + Cast:")
df = df.withColumn("bonus", (col("salary") * 0.10).cast(DoubleType()))

print("Rename Column:")
df = df.withColumnRenamed("name", "employee_name")

print("Drop Column Example:")
df_temp = df.drop("bonus")

print("Conditional Column:")
df = df.withColumn(
    "category",
    when(col("salary") > 60000, "High")
    .when(col("salary") > 50000, "Medium")
    .otherwise("Low")
)

df.show()

# ============================================================
# ADVANCED COLUMN OPERATIONS
# ============================================================

print("IsIn Example:")
df.filter(col("department").isin("HR", "IT")).show()

print("Between Example:")
df.filter(col("salary").between(50000, 65000)).show()

print("Like Example:")
df.filter(col("employee_name").like("A%")).show()

print("Concat Example:")
df = df.withColumn("full_info",
                   concat(col("employee_name"), lit(" - "), col("department")))

# ============================================================
# AGGREGATIONS
# ============================================================

print("Aggregations:")
df.groupBy("department").agg(
    count("*").alias("emp_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    count_distinct("salary").alias("distinct_salary")
).show()

# ============================================================
# SORTING
# ============================================================

print("Order By Salary Desc:")
df.orderBy(col("salary").desc()).show()

# ============================================================
# DISTINCT & DUPLICATES
# ============================================================

print("Distinct Departments:")
df.select("department").distinct().show()

# ============================================================
# JOINS
# ============================================================

dept_data = [
    ("HR", "Human Resource"),
    ("IT", "Information Tech"),
    ("Finance", "Finance Dept")
]

dept_df = spark.createDataFrame(dept_data, ["department", "dept_full_name"])

print("Inner Join:")
df.join(dept_df, "department", "inner").show()

print("Left Join:")
df.join(dept_df, "department", "left").show()

print("Broadcast Join:")
df.join(broadcast(dept_df), "department").show()

# ============================================================
# WINDOW FUNCTIONS
# ============================================================

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

df = df.withColumn("row_number", row_number().over(window_spec)) \
       .withColumn("rank", rank().over(window_spec)) \
       .withColumn("dense_rank", dense_rank().over(window_spec)) \
       .withColumn("lag_salary", lag("salary").over(window_spec)) \
       .withColumn("lead_salary", lead("salary").over(window_spec))

df.show()

# ============================================================
# COMPLEX DATA TYPES
# ============================================================

print("Array + Explode:")
df = df.withColumn("letters", split(col("employee_name"), ""))

df.select("employee_name", explode(col("letters")).alias("letter")).show()

# ============================================================
# SQL OPERATIONS
# ============================================================

df.createOrReplaceTempView("employees")

spark.sql("""
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
""").show()

# ============================================================
# NULL HANDLING
# ============================================================

df_null = df.withColumn("salary",
                        when(col("id") == 2, None).otherwise(col("salary")))

df_null.fillna({"salary": 0}).show()

# ============================================================
# CACHING & PERSISTENCE
# ============================================================

df.cache()
df.persist(StorageLevel.MEMORY_AND_DISK)

print("Count after caching:", df.count())

# ============================================================
# REPARTITION & COALESCE
# ============================================================

print("Partitions Before:", df.rdd.getNumPartitions())

df_repart = df.repartition(4)
print("After Repartition:", df_repart.rdd.getNumPartitions())

df_coalesce = df_repart.coalesce(2)
print("After Coalesce:", df_coalesce.rdd.getNumPartitions())

# ============================================================
# EXECUTION PLAN
# ============================================================

print("Execution Plan:")
df.explain(True)

# ============================================================
# FILE WRITE EXAMPLES (Uncomment to test)
# ============================================================

# df.write.mode("overwrite").csv("output_csv")
# df.write.mode("overwrite").parquet("output_parquet")
# df.write.partitionBy("department").parquet("output_partitioned") 95 cent of dataframe operations ended