# Databricks notebook source
# MAGIC %run "/Workspace/Users/priyankanyemul@gmail.com/epsilon_auto/Epsilon-Auto/Day 3 Spark/productionizing ETL/includes"

# COMMAND ----------

dbutils.widgets.text("catalog","dev")
dbutils.widgets.text("schema","default")
dbutils.widgets.text("tablename","tblname")
catalog=dbutils.widgets.get("catalog")
schema=dbutils.widgets.get("schema")
tablename=dbutils.widgets.get("tablename")

# COMMAND ----------

# DBTITLE 1,Load
#reading
df=spark.read.csv(f"{input_path}{tablename}.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tablename}")

# COMMAND ----------

#dbutils.help()
#dbutils.widgets.help()
