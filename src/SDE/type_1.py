from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

spark = SparkSession.builder \
    .appName("SCD_Type1_Full_Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

# Existing data
existing_data = [
    (1, "Ravi", "Salem"),
    (2, "Anu", "Chennai"),
    (3, "Kumar", "Madurai")
]

columns = ["id", "name", "city"]

dim_df = spark.createDataFrame(existing_data, columns)

dim_df.write.mode("overwrite").saveAsTable("customer_dim")

# New incoming data
new_data = [
    (1, "Ravi", "Coimbatore"),
    (2, "Anu", "Chennai"),
    (4, "Meena", "Trichy")
]

new_df = spark.createDataFrame(new_data, columns)

# Join
merged_df = dim_df.alias("old").join(
    new_df.alias("new"),
    col("old.id") == col("new.id"),
    "fullouter"
)

# SCD Type 1 Logic
scd1_df = merged_df.select(
    coalesce(col("new.id"), col("old.id")).alias("id"),
    coalesce(col("new.name"), col("old.name")).alias("name"),
    coalesce(col("new.city"), col("old.city")).alias("city")
)

scd1_df.show()

# Overwrite warehouse table
scd1_df.write.mode("overwrite").saveAsTable("customer_dim")

spark.table("customer_dim").show()