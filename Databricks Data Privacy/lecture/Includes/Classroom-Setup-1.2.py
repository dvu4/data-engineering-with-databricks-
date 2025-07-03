# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

spark.sql(f'CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.pii_data.pii')

##
## Set paths to user's catalog within the pii_data schema
##

## Set path to DA.paths.stream_source.user_reg to the user's pii_data schema's volume 
## (catalog -> pii_data)
setattr(DA, 'paths.stream_source.user_reg', f'/Volumes/{DA.catalog_name}/pii_data/pii/stream_source/user_reg')
dbutils.fs.mkdirs(DA.paths.stream_source.user_reg)


## Set path to write location DA.paths.stream_source.dialy to the user's pii_data schema's volume 
## (catalog -> pii_data)
setattr(DA, 'paths.stream_source.daily', f'/Volumes/{DA.catalog_name}/pii_data/pii/stream_source/daily')
dbutils.fs.mkdirs(DA.paths.stream_source.daily)

## Set path to working directory location 
## (ops -> volumes -> user volume)
setattr(DA, 'paths.stream_source.daily_working_dir', f'{DA.paths.working_dir}/pii/stream_source/daily')
dbutils.fs.mkdirs(DA.paths.stream_source.daily_working_dir)


# ## Set path to read location
# setattr(DA, 'paths.stream_source.bronze', '/Volumes/dbacademy_gym_data/v01/bronze')


##
## Delete all files/tables prior to the demonstration
##
spark.sql(f'DROP TABLE IF EXISTS {DA.catalog_name}.pii_data.date_lookup_raw')
spark.sql(f'DROP TABLE IF EXISTS {DA.catalog_name}.pii_data.date_lookup_raw')

print('\n------- Resetting Files for the Demo -------\n')
DA.delete_source_files(source_files=DA.paths.stream_source.user_reg)
DA.delete_source_files(source_files=DA.paths.stream_source.daily)
DA.delete_source_files(source_files=DA.paths.stream_source.daily_working_dir)

# Reads the bronze delta table as a spark dataframe from marketplace and creates JSON files in the labusers working dir (ops) with 4 partitions.
DA.load_daily_json(source_dir='/Volumes/dbacademy_gym_data/v01/bronze', 
                   target_path=DA.paths.stream_source.daily_working_dir)


## Load files into the users DA.paths.stream_source.user_reg volume
DA.load(copy_from='/Volumes/dbacademy_gym_data/v01/user-reg', 
        copy_to=f'{DA.paths.stream_source.user_reg}', 
        n=5)

# ## Load files into the users DA.paths.stream_source.user_reg volume
DA.load(copy_from=DA.paths.stream_source.daily_working_dir, 
        copy_to=DA.paths.stream_source.daily, 
        n=2)


## Set default schema to pii
r = spark.sql(f'USE SCHEMA pii_data')

## Create the date_lookup table in user's catalog pii_data schema by copying from marketplace
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {DA.catalog_name}.pii_data.date_lookup_raw
AS
SELECT *
FROM delta.`/Volumes/dbacademy_gym_data/v01/date-lookup`
''')


## Display user values
DA.display_config_values([
        ('Your Course Catalog Name',DA.catalog_name), 
        ('Your Schema','pii_data'),
        ('User Reg Files (DA.paths.stream_source.user_reg)', DA.paths.stream_source.user_reg),
        ('Daily Files (DA.paths.stream_source.daily)', DA.paths.stream_source.daily)
      ]
)