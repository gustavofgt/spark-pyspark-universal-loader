# Databricks notebook source
import json

class UniversalReader:

    ''' 
    This is a generic reader. 
    It's intended to abstract the connection configurations and specific details from the various data sources available, simplifying the process of extracting data.
    
    It expects a Row as input with the following attributes:
        data_source_type: Type of the data source provider
        data_source_name:  Name/Identification of the data source
        host: Usually a hostname or IP from the source server
        port: Server port    

    Specific parameters are expected depending on the data_source_type, example:
        data_source_type = mssql
            - database
            - table
            - user
            - password
        
        data_source_type = adsl (Azure Data Lake Storage):
            - container: container name
            - file_path: folder/directory 
            - file_format: csv, json, parquet, orc...
            - file_pattern (optional): pattern of the file name to search

    '''

    def __init__(self, source_type, source_name ):
        self.data_source_type = source_type
        self.data_source_name = source_name

    # Connect to the specific data source passing it's parameters and returns a 
    def read_from_source(self, config_json) -> "DataFrame":
        print("Verifying data source type")

        if self.data_source_type == "mssql":
            # Connect to
            print("Setting connection to SQL Server")

            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            database = config_json["database"]
            table = config_json["table"]
            query = config_json["query"]
            host = config_json["host"]
            port = config_json["port"]

            # User/Pass automatically retrieved from Databricks secrets according to the source_name
            user = dbutils.secrets.get(self.data_source_name, "user_read")
            password = dbutils.secrets.get(self.data_source_name, "password_read")
            # SQL Server final jdbc connection string
            url = f"jdbc:sqlserver://{host}.database.windows.net:{port};database={database}"
            # If the a query is not provided, then it extracts the whole table
            if query == '':
                query = (f"select * from {table}")

            df = (
                spark.read.format("jdbc")
                .option("driver", driver)
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("query", query)
                .load()
            )
            return (df)

        elif self.data_source_type == "adls":
            
            spark.conf.set("fs.azure.account.key.gustavofgt.dfs.core.windows.net", dbutils.secrets.get(scope=self.data_source_name, key="access_key"))

            try :
                container = config_json["container"]
                file_path = config_json["file_path"]
                file_format = config_json["file_format"]
                file_pattern = config_json["file_pattern"]

                if file_pattern == '' :
                    file_pattern = '*'
                
                url = config_json["url"]

                fullpath = f"abfss://{container}@{url}/{file_path}/{file_pattern}.{file_format}"
                
            except Exception as e:
                print("\nError configuring adls url, commom issue: missing parameter")
                raise(e)
            
            if file_format == 'csv' :
                try:
                    header=config_json["header"]
                    delimiter = config_json["delimiter"]
                    df = spark.read.options(header=header, delim=delimiter).csv(fullpath)
                    return (df)
                except Exception as e:
                    print("\nError configuring adls url, commom issue: missing parameter")
                    raise (e)
                
            
            if file_format == 'json' :
                try:
                    multiline=config_json["multiline"]
                    df = spark.read.options(multiline=multiline).json(fullpath)
                    return (df)
                except Exception as e:
                    raise (e)
                
        elif self.data_source_type == "mongodb":
            # Extractor for Mongo DB
            pass

        elif self.data_source_type == "ssas":
            # Extractor for SQL Server Analysis Services OLAP cube
            pass

# COMMAND ----------

class UniversalWriter:

    """ 
    This is a generic writer. 
    It reads data from a spark dataframe and saves it as defined in the given format an destination_path or table.
    """

    def __init__(self, df, format='delta', destination_path=False):
        self.df = df
        self.format = format
        self.path = destination_path
        
    def write_delta(self, mode, table) -> "None" :

        if table:
            self.df.write.format(self.format).mode(mode).saveAsTable(path = self.path, name=table)
        else:
            self.df.write.format(self.format).mode(mode).save(self.path)

    def merge_by_key_delta(self, table, *keys) -> "None" :
        # Insert or Update based on provided Key Column(s)

        df_source = spark.table(table)
        df_update = self.df
        condition = []
        # Generate merge condition based on *keys parameters
        for key in keys:
            condition.append(f"update.{key} = source.{key}")
        merge_statement = " AND ".join(condition)

        (
            df_source.alias("source")
            .merge(df_update.alias("update"), merge_statement)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )

    def write_file (self, mode, options, flag_single_file=False) -> "None" :
        ''' 
            Writer for all file formats available on spark's write method, such as csv, json and parquet
            Options must be a python dictionary
            flag_single_file defines if the destination file will be partitioned or a single file
        '''
        if flag_single_file :
            df_source.coalesce(1).write.format(self.format).mode(mode).options(**options).save(self.path)
        else :
            df_source.write.format(self.format).mode(mode).options(**options).save(self.path)


# COMMAND ----------

# Read Configuration
def get_connection_config(data_source_name) -> "Row":
    try:
        config = spark.sql(f"select * from spark_catalog.default.data_source_configurations where data_source_name = '{data_source_name}'").collect()[0]
        print("Fetched data source {} configurations: {}".format(config["data_source_name"], config))
        return config
    except IndexError:
        print("No record for the specified data source")
        return False
