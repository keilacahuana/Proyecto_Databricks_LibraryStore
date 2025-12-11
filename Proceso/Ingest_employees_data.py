# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlbsdb1011")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_Proy_fin_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/bookstore_employees.csv"

# COMMAND ----------

df_employees = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

employees_schema = StructType(fields=[StructField("employee_id", IntegerType(), False),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("role", StringType(), True),
                                     StructField("employment_type", StringType(), True),
                                     StructField("hourly_rate", DoubleType(), True),
                                     StructField("hire_date", DateType(), True),
                                     StructField("term_date", DateType(), True),
                                     StructField("dataset", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_employees_final = spark.read\
.option('header', True)\
.schema(employees_schema)\
.csv(ruta)


# COMMAND ----------

# DBTITLE 1,select only specific cols
employees_selected_df = df_employees_final.select(col("employee_id"), 
                                                col("name"), 
                                                col("location"), col("role"), 
                                                col("employment_type"), 
                                                col("hourly_rate"), 
                                                col("hire_date"), 
                                                col("term_date"))


# COMMAND ----------

employees_renamed_df = employees_selected_df.withColumnRenamed("location", "location_desc") \
.withColumnRenamed("role", "role_desc")

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
employees_final_df = employees_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

employees_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.employees")
