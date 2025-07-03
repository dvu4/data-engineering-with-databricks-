# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

@DBAcademyHelper.add_method
def create_user_delete_requests_table(self, schema_name="pii_data"):
    '''
    Create delte requests table.
    '''
    print(f"Creating the user delete requests table within your Catalog's {schema_name} schema.")
        
    r = spark.sql(f'DROP TABLE IF EXISTS {self.catalog_name}.{schema_name}.user_delete_requests')

    r = spark.sql(f'''
    CREATE TABLE {self.catalog_name}.{schema_name}.user_delete_requests (
        mrn BIGINT,
        request_date timestamp
    )
    ''')

    r = spark.sql(f'''
    INSERT INTO {self.catalog_name}.{schema_name}.user_delete_requests (
        mrn, 
        request_date
    )
    VALUES
    (46609218, current_timestamp()),
    (10481208, current_timestamp()),
    (51962029, current_timestamp()),
    (36170414, current_timestamp()),
    (57985430, current_timestamp()),
    (92891095, current_timestamp()),
    (77708899, current_timestamp()),
    (84079469, current_timestamp()),
    (80880484, current_timestamp()),
    (12267606, current_timestamp()),
    (16352917, current_timestamp()),
    (39313172, current_timestamp()),
    (55302643, current_timestamp()),
    (60252784, current_timestamp()),
    (39141616, current_timestamp()),
    (48952287, current_timestamp()),
    (27210934, current_timestamp()),
    (97829250, current_timestamp()),
    (72095631, current_timestamp()),
    (56413066, current_timestamp())
    ''')

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

## Set default schema to pii_data
schema = spark.sql('USE SCHEMA pii_data')

## Create volume in user's catalog within the pii_data schema named CDF demo
spark.sql(f'CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.pii_data.cdf_demo')


##
## Set DA paths
##

## Set path to working directory location to store stream data for CDC (ops -> volumes -> user volume)
setattr(DA, 'paths.stream_source.cdf_demo', f'{DA.paths.working_dir}/cdf_demo/stream_source/cdc')

## Create a cdf_demo/stream_source/cdc folder in the cdf_demo volume in the pii_data schema
setattr(DA,'paths.cdc_stream',f'/Volumes/{DA.catalog_name}/pii_data/cdf_demo/stream_source/cdc')

## Create a cdf_demo/_checkpoints folder in the cdf_demo volume in the pii_data schema
setattr(DA,'paths.checkpoints',f'/Volumes/{DA.catalog_name}/pii_data/cdf_demo/_checkpoints')

##
## Delete any files in the locations path
##
print('\n------- Resetting Files for the Start of the Demo -------\n')
DA.delete_source_files(source_files=DA.paths.stream_source.cdf_demo)
DA.delete_source_files(source_files=DA.paths.cdc_stream)

try:
    dbutils.fs.rm(DA.paths.checkpoints, True)
except:
    pass

##
## Create directories
## 

## Create a folder {DA.paths.working_dir}/cdf_demo/stream_source/cdc folder in the dbacademy.ops user's volume
dbutils.fs.mkdirs(DA.paths.stream_source.cdf_demo)

## Create a folder /Volumes/{DA.catalog_name}/pii_data/cdf_demo/stream_source/cdc within the pii_data schema
dbutils.fs.mkdirs(DA.paths.cdc_stream)

## Create a folder /Volumes/{DA.catalog_name}/pii_data/cdf_demo/_checkpoints within the pii_data schema
dbutils.fs.mkdirs(f'{DA.paths.checkpoints}')


## Load files to dbacademy-ops-uservolume-cdf_demo 
DA.load_cdc_json(target_path=DA.paths.stream_source.cdf_demo)


## Drop table if exists
print('\nDrop the bronze_users table if it exists.')
dropTable = spark.sql(f'DROP TABLE IF EXISTS {DA.catalog_name}.pii_data.bronze_users')

print('\nDrop the delete_requests table if it exists.')
dropTable = spark.sql(f'DROP TABLE IF EXISTS {DA.catalog_name}.pii_data.delete_requests')

# Create user delete requests for delete propagation section
DA.create_user_delete_requests_table()

## Display values for users
DA.display_config_values([
            ('Your Course Catalog Name', DA.catalog_name),
            ('Your Schema Name', 'pii_data'),
            ('Source Data Volume (DA.paths.cdc_stream)', DA.paths.cdc_stream),
            ('Your Checkpoint Location (DA.paths.checkpoints)', DA.paths.checkpoints)
        ])