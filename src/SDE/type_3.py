from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("SCD_Type3_Example") \
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------------------------------
# 1️⃣ Existing Warehouse Table
# --------------------------------------------------

existing_data = [
    (1, "Ravi", "Salem", None),
    (2, "Anu", "Chennai", None)
]
# Define the schema explicitly with types
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("current_city", StringType(), True),
    StructField("previous_city", StringType(), True)
])

# columns = ["id", "name", "current_city", "previous_city"]

dim_df = spark.createDataFrame(existing_data, schema)

dim_df.write.mode("overwrite").saveAsTable("customer_dim_scd3")

print("Initial Table")
spark.table("customer_dim_scd3").show()

# --------------------------------------------------
# 2️⃣ New Incoming Data
# --------------------------------------------------

new_data = [
    (1, "Ravi", "Coimbatore"),  # changed
    (2, "Anu", "Chennai"),      # same
    (3, "Meena", "Trichy")      # new
]
new_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
])
# new_columns = ["id", "name", "city"]

new_df = spark.createDataFrame(new_data, new_schema)

# --------------------------------------------------
# 3️⃣ Read Existing Table
# --------------------------------------------------

old_df = spark.table("customer_dim_scd3")

# --------------------------------------------------
# 4️⃣ Join
# --------------------------------------------------

merged_df = old_df.alias("old").join(
    new_df.alias("new"),
    col("old.id") == col("new.id"),
    "fullouter"
)
# print("merfes")
merged_df.show()

# --------------------------------------------------
# 5️⃣ Apply SCD Type 3 Logic
# --------------------------------------------------

scd3_df = merged_df.select(
    coalesce(col("new.id"), col("old.id")).alias("id"),

    coalesce(col("new.name"), col("old.name")).alias("name"),

    # current_city logic
    when(col("old.id").isNull(), col("new.city"))  # new record
    .when(col("old.current_city") != col("new.city"), col("new.city"))  # changed
    .otherwise(col("old.current_city"))
    .alias("current_city"),

    # previous_city logic
    when(col("old.id").isNull(), None)  # new record
    .when(col("old.current_city") != col("new.city"), col("old.current_city"))  # changed
    .otherwise(col("old.previous_city"))
    .alias("previous_city")
)
scd3_df.show()

# --------------------------------------------------
# 6️⃣ Overwrite Table
# --------------------------------------------------

# Fix: Write to a temporary staging table, then use INSERT OVERWRITE
staging_table_name = "customer_dim_scd3_staging"
scd3_df.write.mode("overwrite").saveAsTable(staging_table_name)

spark.sql(f"INSERT OVERWRITE TABLE customer_dim_scd3 SELECT id, name, current_city, previous_city FROM {staging_table_name}")

# Clean up the staging table
spark.sql(f"DROP TABLE IF EXISTS {staging_table_name}")

spark.table("customer_dim_scd3").show()