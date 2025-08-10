# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from delta.tables import DeltaTable

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True)
])

# Initial data for the delta table
data = [
    (1, "Alice", "HR", 60000, "New York", "1234567890", "alice@abc.com"),
    (2, "Bob", "IT", 80000, "San Francisco", "2345678901", "bob@abc.com"),
    (3, "Charlie", "Finance", 75000, "Chicago", "3456789012", "charlie@abc.com"),
    (4, "David", "Marketing", 65000, "Boston", "4567890123", "david@abc.com")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Write to Delta Table
df.write.format("delta").mode("overwrite").saveAsTable("dim_employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# Read the incoming CSV file directly into a DataFrame
incoming_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/mnt/input-data/emp_incoming.csv")
)


# COMMAND ----------

incoming_df.display()

# COMMAND ----------

# Reference the target Delta table
target_table = DeltaTable.forName(spark, "dim_employee")

# Perform the SCD Type 1 merge
(
    target_table.alias("target")
    .merge(
        source=incoming_df.alias("source"),
        condition="target.emp_id = source.emp_id"
    )
    .whenMatchedUpdate(set={
        "name": "source.name",
        "department": "source.department",
        "salary": "source.salary",
        "city": "source.city",
        "phone": "source.phone",
        "email": "source.email"
    })
    .whenNotMatchedInsert(values={
        "emp_id": "source.emp_id",
        "name": "source.name",
        "department": "source.department",
        "salary": "source.salary",
        "city": "source.city",
        "phone": "source.phone",
        "email": "source.email"
    })
    .execute()
)

display(spark.sql("SELECT * FROM dim_employee ORDER BY emp_id"))


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dim_employee ORDER BY emp_id;
