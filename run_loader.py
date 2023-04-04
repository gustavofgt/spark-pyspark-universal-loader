# Databricks notebook source
# MAGIC %run ./UniversalLoader

# COMMAND ----------

# DBTITLE 1,Source Parameters
dbutils.widgets.text("source_name", "wwimporters", "01. Source Name: ")

data_source_name = dbutils.widgets.get("source_name")



# COMMAND ----------

# DBTITLE 1,Sink Parameters
dbutils.widgets.text("destination_format", "delta", "02. Destination Format: ")
dbutils.widgets.text("destination_path", "/tmp/delta", "03. Destination Path: ")
dbutils.widgets.text("destination_database", "wwimporters", "04. Destination Database: ")
dbutils.widgets.text("destination_table", "sales_orders", "05. Destination Table: ")
dbutils.widgets.text("write_mode", "append", "07. Write Mode ")
dbutils.widgets.text("key_columns", "", "8. Key Columns ")
dbutils.widgets.text("options", "", "9. Options ")

destination_format = dbutils.widgets.get("destination_format")
destination_path = dbutils.widgets.get("destination_path")
destination_database = dbutils.widgets.get("destination_database")
destination_table = dbutils.widgets.get("destination_table")
write_mode = dbutils.widgets.get("write_mode")
key_columns = dbutils.widgets.get("key_columns")
options = dbutils.widgets.get("options")


# COMMAND ----------

# Read data source configuration from table default.data_source_configurations

source_config = get_connection_config(data_source_name = data_source_name, object_name=destination_table)
source_type = source_config["data_source_type"]
options = json.loads(source_config["options"])


# COMMAND ----------

# Instantiate UniversalReader object and call read_from_source method. Display top(10) records only for sampling
loader = UniversalReader(source_type=source_type, source_name=data_source_name)
df_source = loader.read_from_source(config_json=options)
print("\nSample:")
df_source.limit(10).display()

# COMMAND ----------

# Writer

writer = UniversalWriter(
    df=df_source,
    database=destination_database,
    table=destination_table,
    data_format=destination_format,
    destination_path=destination_path,
)

if destination_format.lower() != 'delta' :
    writer.write_file(mode = write_mode, options=options)
elif write_mode == 'merge' :
    keys = key_columns.strip().split(',')
    writer.merge_delta(*keys)
else :
    writer.write_delta(mode = write_mode)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.data_source_configurations
