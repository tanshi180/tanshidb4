# Databricks notebook source
# MAGIC %md
# MAGIC managed tables            unmanaged(external) tables

# COMMAND ----------

data = [(1, "Alice", 5000),
        (2, "Bob", 6000),
        (3, "Charlie", 7000),
        (4, "David", 8000)]
columns = ["id", "name", "salary"]

df = spark.createDataFrame(data,columns)

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("employee_managed")

# COMMAND ----------

spark.sql("DESCRIBE DETAIL employee_managed").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_managed;

# COMMAND ----------

df33 = spark.sql("select * from employee_managed")
df33.display()

# COMMAND ----------

# MAGIC %md
# MAGIC external

# COMMAND ----------

external_path = "/mnt/input-data/delta/employee_external"

df.write.format("delta").mode("overwrite").save(external_path)

spark.sql(f""" create table employee_external using delta location'{external_path}'""")

# COMMAND ----------

spark.sql("DESCRIBE DETAIL employee_external").display()
