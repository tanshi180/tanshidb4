# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# Define schema with SCD Type 2 fields
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("phone", LongType(), True),
    StructField("email", StringType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True)
])

# Initial data
data = [
    (1, "Alice", "HR", 60000, "New York", 1234567890, "alice@abc.com", True, datetime.now(), None),
    (2, "Bob", "IT", 80000, "San Francisco", 2345678901, "bob@abc.com", True, datetime.now(), None),
    (3, "Charlie", "Finance", 75000, "Chicago", 3456789012, "charlie@abc.com", True, datetime.now(), None),
    (4, "David", "Marketing", 65000, "Boston", 4567890123, "david@abc.com", True, datetime.now(), None)
]

df = spark.createDataFrame(data, schema)

# Write to Delta Table
df.write.format("delta").mode("overwrite").saveAsTable("dim_employee2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee2

# COMMAND ----------

incoming_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/mnt/input-data/emp_incoming.csv")
)

# COMMAND ----------

incoming_df.display()

# COMMAND ----------


# Load target table
delta_target = DeltaTable.forName(spark, "dim_employee2")

# 1. Find records that need to be updated (data changed)
updates_df = incoming_df.alias("incoming").join(
    spark.table("dim_employee2").alias("target"),
    on="emp_id"
).filter("target.is_current = true AND (incoming.name <> target.name OR incoming.department <> target.department OR incoming.salary <> target.salary OR incoming.city <> target.city OR incoming.phone <> target.phone OR incoming.email <> target.email)") \
.select("incoming.*")

# 2. Expire the old versions (set is_current = false and end_date = current_timestamp)
(
    delta_target.alias("target")
    .merge(
        updates_df.alias("incoming"),
        "target.emp_id = incoming.emp_id AND target.is_current = true"
    )
    .whenMatchedUpdate(set={
        "is_current": lit(False),
        "end_date": current_timestamp()
    })
    .execute()
)

# 3. Insert new records (from updates + new inserts)
from pyspark.sql.functions import expr

# Add SCD columns
new_rows_df = incoming_df.alias("incoming") \
.join(
    spark.table("dim_employee2").filter("is_current = true").alias("target"),
    on="emp_id",
    how="leftanti"  # selects only new or changed rows
).union(updates_df) \
.withColumn("is_current", lit(True)) \
.withColumn("start_date", current_timestamp()) \
.withColumn("end_date", lit(None).cast("timestamp"))

# Insert the new records
new_rows_df.write.format("delta").mode("append").saveAsTable("dim_employee2")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee2
