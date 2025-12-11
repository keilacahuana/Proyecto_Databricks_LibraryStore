# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import * #StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/bookstore_payroll.csv"

# COMMAND ----------

df_payroll = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

payroll_schema = StructType(fields=[StructField("pay_period_start", DateType(), True),
                                  StructField("employee_id", IntegerType(), True),
                                  StructField("employee_name", StringType(), True),
                                  StructField("location", StringType(), True),
                                  StructField("role", StringType(), True),
                                  StructField("employment_type", StringType(), True),
                                  StructField("hourly_rate", DoubleType(), True),
                                  StructField("hours_biweekly", DoubleType(), True),
                                  StructField("gross_pay", DoubleType(), True),
                                  StructField("cpp_withheld", DoubleType(), True),
                                  StructField("ei_withheld", DoubleType(), True),
                                  StructField("income_tax_withheld", DoubleType(), True),
                                  StructField("employee_benefits", DoubleType(), True),
                                  StructField("net_pay", DoubleType(), True),
                                  StructField("dataset", StringType(), True) 
])

# COMMAND ----------

payroll_df = spark.read \
            .option("header", True) \
            .schema(payroll_schema) \
            .csv(ruta)

# COMMAND ----------

payroll_with_timestamp_df = payroll_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

payroll_selected_df = payroll_with_timestamp_df.select(col('employee_id'), 
                                                   col('hours_biweekly').alias('hours_bw'), 
                                                   col('pay_period_start'), col('gross_pay'),
                                                   col('cpp_withheld'), col('ei_withheld'),
                                                   col('income_tax_withheld'),
                                                   col('employee_benefits'),col('net_pay'), col('ingestion_date'))

# COMMAND ----------

# MAGIC  %skip
# MAGIC  payroll_selected_df.display()

# COMMAND ----------

payroll_selected_df.write.option("overwriteSchema", "true").mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.payroll')
