# Databricks notebook source
# MAGIC %md
# MAGIC # Description
# MAGIC 
# MAGIC The notebook **Universal Loader** contains the following components:
# MAGIC 
# MAGIC **get_connection_config** function: this function is responsible for reading the data source datails from the table default.data_source_configurations
# MAGIC     . It expects the data_source_name and object_name as arguments. The later would usually be the name of the final (and normally also the source) table
# MAGIC     . It returns the contents of the specific source-table to be used by the extractor class UniversalReader
# MAGIC 
# MAGIC **UniversalReader** class: provides methods for reading from different data sources. Currently only reads from SQL Server and ADLS but it can be extended as required. 
# MAGIC     . I hadn't enough time to set up an OLAP data source so I didn't create its reader, but would be similar to the others, just passing the connection parameters and possibly a MDX query.
# MAGIC     . The mandatory parameters varies from source to source and must be properly defined in the options field on the data_source_configurations table
# MAGIC 
# MAGIC **UniversalWriter** class: provides distinct methods for storing the data into databricks and also can be extended
# MAGIC     . The parameters are currently being passed through databricks widgets but could also be stored in somewhere else like a table or config file
# MAGIC 
# MAGIC 
# MAGIC I'v created a workflow as an example of how it could be executed:
# MAGIC https://adb-8637055435755248.8.azuredatabricks.net/?o=8637055435755248#job/308275570447876/tasks/task/ingest_users_csv
# MAGIC 
# MAGIC In that matter we could use other applications like Data Factory as well. Since the code is in pyspark it can be easily portable/itegrated to other applications.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC As a last statement, the code is very simple and has lots of gaps, but from the lack of time to the complexity/broadness of the requirements - including setting up this testing environment - it may be a starting point to a bigger project or at least give some ideas. :)
# MAGIC 
# MAGIC Bellow some queries on the tables:       

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.users

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwimporters.sales_orders
