from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

spark = SparkSession.builder \
    .appName("SCD_Type2_Full_Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------------------------------
# 1️⃣ Create Existing Dimension Table (Warehouse)
#    Using Proper Schema
# --------------------------------------------------

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("current_flag", StringType(), True)
])

existing_data = [
    (1, "Ravi", "Salem", date(2023, 1, 1), None, "Y"),
    (2, "Anu", "Chennai", date(2023, 1, 1), None, "Y"),
    (3, "Kumar", "Madurai", date(2023, 1, 1), None, "Y")
]

dim_df = spark.createDataFrame(existing_data, schema)

dim_df.write.mode("overwrite").saveAsTable("customer_dim_scd2")

print("Initial Warehouse Table")
spark.table("customer_dim_scd2").show()


# --------------------------------------------------
# 2️⃣ New Incoming Data
# --------------------------------------------------

new_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])

new_data = [
    (1, "Ravi", "Coimbatore"),
    (2, "Anu", "Chennai"),
    (4, "Meena", "Trichy")
]

new_df = spark.createDataFrame(new_data, new_schema)


# --------------------------------------------------
# 3️⃣ Read Current Active Records
# --------------------------------------------------

old_df = spark.table("customer_dim_scd2") \
    .filter(col("current_flag") == "Y")


# --------------------------------------------------
# 4️⃣ Join
# --------------------------------------------------

join_df = old_df.alias("old").join(
    new_df.alias("new"),
    col("old.id") == col("new.id"),
    "fullouter"
)


# --------------------------------------------------
# 5️⃣ Identify Changed Records
# --------------------------------------------------

changed_df = join_df.filter(
    (col("old.city") != col("new.city")) &
    col("new.city").isNotNull()
)


# --------------------------------------------------
# 6️⃣ Expire Old Records
# --------------------------------------------------

expired_df = changed_df.select(
    col("old.id"),
    col("old.name"),
    col("old.city"),
    col("old.start_date"),
    current_date().alias("end_date"),
    lit("N").alias("current_flag")
)


# --------------------------------------------------
# 7️⃣ Insert New Records
# --------------------------------------------------

new_records_df = join_df.filter(
    col("new.id").isNotNull()
).select(
    col("new.id").alias("id"),
    col("new.name").alias("name"),
    col("new.city").alias("city"),
    current_date().alias("start_date"),
    lit(None).cast(DateType()).alias("end_date"),
    lit("Y").alias("current_flag")
)



# --------------------------------------------------
# 8️⃣ Keep Unchanged Records
# --------------------------------------------------

unchanged_df = old_df.alias("old").join(
    changed_df.select(col("old.id").alias("id")),
    "id",
    "left_anti"
)


# --------------------------------------------------
# 9️⃣ Final Dimension Table
# --------------------------------------------------

final_df = unchanged_df.union(expired_df).union(new_records_df)

print("Final SCD Type 2 Table")
final_df.show()


# --------------------------------------------------
# 🔟 Overwrite Warehouse Table
# --------------------------------------------------

final_df.write.mode("overwrite").saveAsTable("customer_dim_scd2")

spark.table("customer_dim_scd2").show()