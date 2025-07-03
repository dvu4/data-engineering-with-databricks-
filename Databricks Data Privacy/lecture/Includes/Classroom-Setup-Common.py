# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

## This will create a DA variable in SQL to use in SQL queries.
## Queries the dbacademy.ops.meta table and uses those key-value pairs to populate the DA variable for SQL queries.
## Variable can't be used with USE CATALOG in databricks.

# create_temp_view = '''
# CREATE OR REPLACE TEMP VIEW user_info AS
# SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
# FROM dbacademy.ops.meta
# '''

# declare_variable = 'DECLARE OR REPLACE DA MAP<STRING,STRING>'

# set_variable_from_view = 'SET VAR DA = (SELECT * FROM user_info)'

# spark.sql(create_temp_view)
# spark.sql(declare_variable)
# spark.sql(set_variable_from_view)

# ## Drop the view after the creation of the DA variable
# spark.sql('DROP VIEW IF EXISTS user_info');

# COMMAND ----------

@DBAcademyHelper.add_method
def load(self, copy_from, copy_to, n, sleep=2):
  '''
  Copy files from one location to another destination's volume.
  
  Parameters:
  - copy_from (str): The source directory where files are to be copied from.
  - copy_to (str): The destination directory where files will be copied to.
  - n (int): The number of files to copy from the source.
  - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.
  
  This method performs the following tasks:
    1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
    2. Verifies that the source directory has at least `n` files.
    3. Copies files from the source to the destination, skipping files already present at the destination.
    4. Pauses for `sleep` seconds after copying each file.
    5. Stops after copying `n` files or if all files are processed.
    6. Will print information on the files copied.

  Example:
  load(copy_from='/Volumes/gym_data/v01/user-reg', 
       copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',   ## Uses the user's default volume in dbacademy/ops/vocareumlab@name
       n=1)
  '''
  import os
  import time

  print("\n----Loading files to user's volume-----")

  ## List all files in the copy_from volume and sort the list
  list_of_files_to_copy = sorted(os.listdir(copy_from))
  total_files_in_copy_location = len(list_of_files_to_copy)

  ## Get a list of files in the source
  list_of_files_in_source = os.listdir(copy_to)

  assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."

  ## Looping counter
  counter = 1

  ## Load files if not found in the co
  for file in list_of_files_to_copy:

    ## If the file is found in the source, skip it with a note. Otherwise, copy file.
    if file in list_of_files_in_source:
      print(f'File number {counter} - {file} is already in the source volume. Skipping file.')
    else:
      file_to_copy = f'{copy_from}/{file}'
      copy_file_to = f'{copy_to}/{file}'
      print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
      dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
      
      ## Sleep after load
      time.sleep(sleep) 

    ## Stop after n number of loops based on argument.
    if counter == n:
      print(f"Stop loading files to user's volume.")
      break
    else:
      counter = counter + 1

## Example:
# load(copy_from='/Volumes/gym_data/v01/user-reg', 
#      copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg', 
#      n=2)

# COMMAND ----------

@DBAcademyHelper.add_method
def delete_source_files(self, source_files):
  """
  Deletes all files in the specified source volume.

  This function iterates through all the files in the given volume,
  deletes them, and prints the name of each file being deleted.

  Parameters:
  ----------
  source_files : str, optional
      The path to the volume containing the files to delete. 
      Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
        Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

  Returns:
  -------
  None
      This function does not return any value. It performs file deletion as a side effect and prints all files that it deletes.

  Example:
  --------
  delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
  """
  
  import os
  
  print(f'\nSearching for files in {source_files} to delete...')
  if os.path.exists(source_files):
    list_of_files = sorted(os.listdir(source_files))
  else:
    list_of_files = None

  if not list_of_files:  # Checks if the list is empty.
    print(f"No files found in {source_files}.\n")
  else:
    for file in list_of_files:
      file_to_delete = source_files + '/' + file
      print(f'Deleting file: {file_to_delete}')
      dbutils.fs.rm(file_to_delete)

# COMMAND ----------

@DBAcademyHelper.add_method
def create_customer_bronze(self):
  '''
  Create the customers_bronze table in the user's catalog from the dbacademy_retail catalog.
  '''

  drop_table = spark.sql(f'DROP TABLE IF EXISTS {self.catalog_name}.pii_data.customers_bronze')

  create_table = spark.sql(f'''
    CREATE TABLE {self.catalog_name}.pii_data.customers_bronze
    AS
    SELECT *
    FROM dbacademy_retail.v01.customers
  ''')

  print(f'NOTE: Table created: {self.catalog_name}.pii_data.customers_bronze')




@DBAcademyHelper.add_method
def create_customer_silver(self):
  '''
  Create the customers_silver table in the user's catalog from the dbacademy_retail catalog.
  '''

  drop_table = spark.sql(f'DROP TABLE IF EXISTS {self.catalog_name}.pii_data.customers_silver')

  create_table = spark.sql(f'''
    CREATE TABLE {self.catalog_name}.pii_data.customers_silver
    AS
    SELECT *
    FROM dbacademy_retail.v01.customers
  ''')

  print(f'NOTE: Table created: {self.catalog_name}.pii_data.customers_silver')

# COMMAND ----------

@DBAcademyHelper.add_method
def load_daily_json(self, source_dir, target_path):  
    '''
    Create daily json files.
    '''

    df = (spark
          .read
          .load(source_dir)
          .withColumn("day", F.when(F.col("date") <= '2019-12-01', 1).otherwise(F.dayofmonth("date")))
          .filter(F.col("Topic") == 'user_info')
        #   .filter(F.col("day") >= batch_start)
        #   .filter(F.col("day") <= batch_end)
          .drop("date", "week_part", "day")  
    )

    (df
     .repartition(4,'timestamp')
     .write
     .mode("overwrite")
     .format("json")
     .save(target_path))
    
    ## Delete any files that are not necessary (begin with _, like _committed, _success)
    files = dbutils.fs.ls(self.paths.stream_source.daily_working_dir)
    for f in files:
        file_path = f.path
        file_name = f.name
        if file_name.startswith('_'):
            dbutils.fs.rm(file_path)

# COMMAND ----------

@DBAcademyHelper.add_method
def load_cdc_json(self, target_path):  
    '''
    Create CDC stream source json files from marketplace to user's working directory.
    '''

    import os
    import time

    ## Read the delta files in the marketplace share
    source_dir = '/Volumes/dbacademy_gym_data/v01/pii/raw/'

    df = (
        spark
        .read
        .load(source_dir)
    )

    print('\n-----Creating JSON files for CDC-----')
    ## Create 3 json files, one for each batch.
    for batch in [1,2,3]:
        (df
        .repartition(1)
        .filter(df['batch']== batch)
        .write
        .mode("append")
        .json(f'{target_path}')
        )
        
        ## Rename files in order by batch
        files = dbutils.fs.ls(target_path)
        for f in files:
            if f.name.startswith('part') == True:
                volume_file_path = f.path.split(':')[1]
                new_file_name = volume_file_path.replace(f.name,f'batch0{batch}.json')
                os.rename(volume_file_path, new_file_name)
                print(f"Creating file {new_file_name}.")

    # Delete any files that are not necessary (begin with _, like _committed, _success)
    files = dbutils.fs.ls(target_path)
    for f in files:
        file_path = f.path
        file_name = f.name
        if file_name.startswith('_'):
            dbutils.fs.rm(file_path)

# COMMAND ----------

@DBAcademyHelper.add_method
def generate_pipeline(self,
                      pipeline_name, 
                      notebooks_folder, 
                      pipeline_notebooks,
                      use_schema,
                      use_configuration = None,
                      use_serverless = True,
                      use_continuous = False):
    """
    Generates a Databricks pipeline based on the specified configuration parameters.

    This method creates a pipeline that can execute a series of notebooks in a serverless environment, 
    and allows the option to use continuous runs if needed. It relies on Databricks SDK to interact with Databricks services.

    By default will use the dbacademy catalog, within the user's specific schema.

    Parameters:
    - pipeline_name (str): The name of the pipeline to be created. The value is concatenated with your catalog name.
    - notebooks_folder (str): The folder within Databricks where the notebooks are stored. This should be the folder name one level above where the Classroom-Setup-Common notebooks lives.
    - pipeline_notebooks (list of str): List of notebook paths that should be included in the pipeline. Use path from after the notebooks_folder.
    - use_configuration (dict or None): Optional configuration dictionary that can be used to customize the pipeline's settings. 
        - Default is None.
    - use_serverless (bool): Flag indicating whether to use the serverless environment for the pipeline. 
        - Defaults to True.
    - use_continuous (bool): Flag indicating whether to set up continuous execution for the pipeline. 
        - Defaults to False.

    Returns:
    - pipeline (object): A Databricks pipeline object created using the specified parameters.
    - Stores the pipeline_id in the self.current_pipeline_id attribute.

    Raises:
    - Raises an error if the pipeline name already exists.

    Example usage:
            DA.generate_pipeline(
                pipeline_name=f"DEV",            ## creates a pipeline catalogname_DEV
                use_schema = 'default',          ## uses schema within user's catalog
                notebooks_folder='Pipeline 01', 
                pipeline_notebooks=[            ## Uses Pipeline 01/bronze/dev/ingest_subset
                    'bronze/dev/ingest_subset',
                    'silver/quarantine'
                    ]
                )
    
    Notes:
    - The method imports the necessary Databricks SDK service for pipelines.
    - The 'use_catalog' and 'use_schema' attributes are assumed to be part of the class, and are used to define catalog and schema name using the customer DA object attributes.
    """
    import os
    from databricks.sdk.service import pipelines
    
    ## Set pipeline name and target catalog
    pipeline_name = f"{self.catalog_name}_{pipeline_name}" 
    self.pipeline_name = pipeline_name
    
    use_catalog = f"{self.catalog_name}"
    
    ## Check for duplicate name. Return error if name already found.
    for pipeline in self.workspace.pipelines.list_pipelines():
      if pipeline.name == pipeline_name:
        assert_false = False
        assert assert_false, f'You already have pipeline named {pipeline_name}. Please go to the Delta Live Tables page and manually delete the pipeline. Then rerun this program to create the pipeline.'

    ## Get path of includes folder
    current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

    ## Go back one folder to the main course folder then navigate to the folder specified by the notebooks_folder variable
    main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1]) + '/' + notebooks_folder

    ## Create paths for each notebook specified in method argument notebooks(list of notebooks to use)
    notebooks_paths = []
    for i, notebook in enumerate(pipeline_notebooks):
        current_notebook_path = (f'Notebook #{i + 1}', main_course_folder_path + '/' + notebook)
        
        # Attempt to list the contents of the path. If the path does not exist return an error.
        if os.path.exists(current_notebook_path[1]):
            pass
        else:
            assert_false = False
            assert assert_false, f'The notebook path you specified does not exists {current_notebook_path[1]}. Please specify a correct path in the generate_pipeline() method using the notebooks_folder and pipeline_notebooks arguments. Read the method documentation for more information.'
        
        notebooks_paths.append(current_notebook_path)


    ## Create pipeline
    pipeline_info = self.workspace.pipelines.create(
        allow_duplicate_names=True,
        name=pipeline_name,
        catalog=use_catalog,
        target=use_schema,
        serverless=use_serverless,
        continuous=use_continuous,
        configuration=use_configuration,
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook)) for i, notebook in notebooks_paths]
    )

    ## Store pipeline ID
    self.current_pipeline_id = pipeline_info.pipeline_id 

    ## Success message
    print(f"Created the DLT pipeline {pipeline_name} using the settings from below:\n")

    ## Use the display_config_values function to display the following values as HTML output.
    ## Will list the Job Name and notebook paths.
    self.display_config_values([
            ('DLT Pipeline Name', pipeline_name),
            ('Using Catalog', self.catalog_name),
            ('Using Schema', use_schema),
            ('Compute', 'Serverless' if use_serverless else 'Error in setting Compute')
        ] + notebooks_paths)


# # TEST METHOD
# DA.generate_pipeline(
#     pipeline_name=f"DEV1", 
#     use_schema = 'default',
#     notebooks_folder='Pipeline 01', 
#     pipeline_notebooks=[
#         'bronze/dev/ingest_subset',
#         'silver/quarantine'
#         ]
#     )

# COMMAND ----------

@DBAcademyHelper.add_method
def start_pipeline(self, start_pipeline_id='current'):
  '''
  Starts the pipeline using the attribute set from the generate_pipeline() method.
  '''
  if start_pipeline_id == 'current':
    self.workspace.pipelines.start_update(self.current_pipeline_id)
    print(f'Starting pipeline {self.pipeline_name}, ID {self.current_pipeline_id}')
  else:
    print("No DLT pipeline found to start. The pipeline ID is stored when executing the 'generate_pipeline' method. Your variable might of been cleared.")
    print("TO DO: Try navigating to your DLT pipeline and obtain the pipeline ID, and then setting the 'start_pipeline_id' variable to that ID as a string.")

# COMMAND ----------

@DBAcademyHelper.add_method
def create_users_bin_table(self, schema_name):
    '''
    Creating the users table in the specified schema within the user's unique catalog.
    '''


    print(f"Creating the users_bin table within your Catalog's {schema_name} schema.")
        
    r = spark.sql(f'DROP TABLE IF EXISTS {self.catalog_name}.{schema_name}.users_bin')

    r = spark.sql(f'''
    CREATE TABLE {self.catalog_name}.{schema_name}.users_bin (
        user_id BIGINT,
        age STRING,
        gender STRING,
        city STRING,
        state STRING
    )
    ''')

    r = spark.sql(f'''
    INSERT INTO {self.catalog_name}.{schema_name}.users_bin (
        user_id, 
        age, 
        gender, 
        city, 
        state
    )
    VALUES
    (26285, '45-55', 'M', 'Beverly Hills', 'CA') ,
    (38766, '35-45', 'M', 'Los Angeles', 'CA') ,
    (41367, '85-95', 'M', 'Pacoima', 'CA') ,
    (28938, '35-45', 'F', 'Los Angeles', 'CA') ,
    (35226, '65-75', 'M', 'Glendale', 'CA') ,
    (45875, '85-95', 'M', 'El Monte', 'CA') ,
    (42794, '35-45', 'M', 'Paramount', 'CA') ,
    (15149, '45-55', 'M', 'Los Angeles', 'CA') ,
    (37012, '95+', 'M', 'Torrance', 'CA') ,
    (41954, '55-65', 'F', 'Glendale', 'CA') ,
    (30088, '25-35', 'M', 'Los Angeles', 'CA') ,
    (31362, '18-25', 'F', 'Los Angeles', 'CA') ,
    (26847, '25-35', 'M', 'Lakewood', 'CA') ,
    (33987, '55-65', 'F', 'Canyon Country', 'CA') ,
    (30612, '85-95', 'M', 'Valyermo', 'CA') ,
    (42387, '65-75', 'F', 'Playa Vista', 'CA') ,
    (14508, '85-95', 'M', 'Sierra Madre', 'CA') ,
    (34740, '25-35', 'M', 'Santa Monica', 'CA') ,
    (19859, '25-35', 'F', 'Santa Clarita', 'CA') ,
    (29213, '95+', 'F', 'North Hollywood', 'CA') ,
    (27306, '55-65', 'F', 'Edwards', 'CA') ,
    (32018, '55-65', 'M', 'Los Angeles', 'CA') ,
    (27703, '75-85', 'M', 'Woodland Hills', 'CA') ,
    (19005, '75-85', 'F', 'Studio City', 'CA') ,
    (12474, '75-85', 'M', 'San Fernando', 'CA') ,
    (47403, '35-45', 'F', 'Carson', 'CA') ,
    (28588, '35-45', 'M', 'Arcadia', 'CA') ,
    (35428, '65-75', 'F', 'Rowland Heights', 'CA') ,
    (19103, '35-45', 'F', 'Burbank', 'CA') ,
    (17217, '25-35', 'F', 'Sierra Madre', 'CA') ,
    (23916, '95+', 'M', 'Signal Hill', 'CA') ,
    (49075, '35-45', 'M', 'Woodland Hills', 'CA') ,
    (19888, '85-95', 'M', 'Los Alamitos', 'CA') ,
    (41192, '35-45', 'M', 'Inglewood', 'CA') ,
    (27671, '85-95', 'M', 'Glendale', 'CA') ,
    (48913, '65-75', 'M', 'Los Angeles', 'CA') ,
    (40559, '65-75', 'M', 'Glendale', 'CA') ,
    (40872, '35-45', 'M', 'Los Angeles', 'CA') ,
    (49271, '95+', 'M', 'Arcadia', 'CA') ,
    (16093, '35-45', 'F', 'Northridge', 'CA') ,
    (33733, '55-65', 'M', 'Rowland Heights', 'CA') ,
    (49661, '85-95', 'F', 'Santa Clarita', 'CA') ,
    (29130, '55-65', 'M', 'Los Angeles', 'CA') ,
    (31057, '18-25', 'F', 'Pacoima', 'CA') ,
    (30514, '75-85', 'F', 'Calabasas', 'CA') ,
    (24863, '85-95', 'M', 'Long Beach', 'CA') ,
    (49835, '85-95', 'M', 'Alhambra', 'CA') ,
    (25477, '55-65', 'M', 'Pomona', 'CA') ,
    (16819, '85-95', 'M', 'Newhall', 'CA') ,
    (35728, '55-65', 'F', 'Montebello', 'CA') ,
    (43104, '45-55', 'F', 'Long Beach', 'CA') ,
    (14232, '35-45', 'M', 'North Hollywood', 'CA') ,
    (19787, '25-35', 'M', 'Northridge', 'CA') ,
    (11745, '65-75', 'F', 'Long Beach', 'CA') ,
    (25143, '75-85', 'M', 'Palmdale', 'CA') ,
    (20496, '35-45', 'M', 'Redondo Beach', 'CA') ,
    (49296, '25-35', 'M', 'Pacoima', 'CA') ,
    (13559, '35-45', 'F', 'Santa Monica', 'CA') ,
    (14633, '25-35', 'F', 'Gardena', 'CA') ,
    (22480, '55-65', 'F', 'Canyon Country', 'CA') ,
    (19622, '85-95', 'F', 'Playa Vista', 'CA') ,
    (20217, '25-35', 'F', 'Long Beach', 'CA') ,
    (27836, '75-85', 'F', 'Covina', 'CA') ,
    (19548, '45-55', 'F', 'Los Angeles', 'CA') ,
    (47261, '75-85', 'M', 'Culver City', 'CA') ,
    (12140, '18-25', 'M', 'Pearblossom', 'CA') ,
    (29745, '18-25', 'F', 'Edwards', 'CA') ,
    (41732, '45-55', 'M', 'Long Beach', 'CA') ,
    (41796, '75-85', 'M', 'Santa Clarita', 'CA') ,
    (43052, '75-85', 'F', 'Beverly Hills', 'CA') ,
    (24018, '75-85', 'F', 'Beverly Hills', 'CA') ,
    (23250, '65-75', 'M', 'Pinon Hills', 'CA') ,
    (28814, '85-95', 'F', 'Los Angeles', 'CA') ,
    (36590, '35-45', 'F', 'Glendale', 'CA') ,
    (47250, '35-45', 'M', 'South Gate', 'CA') ,
    (19150, '25-35', 'F', 'Carson', 'CA') ,
    (47356, '25-35', 'M', 'La Crescenta', 'CA') ,
    (12227, '65-75', 'F', 'Los Angeles', 'CA') ,
    (17807, '45-55', 'M', 'Los Angeles', 'CA') ,
    (24968, '85-95', 'F', 'Los Angeles', 'CA') ,
    (36117, '45-55', 'F', 'Los Angeles', 'CA') ,
    (36644, '75-85', 'M', 'Altadena', 'CA') ,
    (41488, '65-75', 'M', 'Monrovia', 'CA') ,
    (40177, '85-95', 'M', 'Avalon', 'CA') ,
    (21896, '85-95', 'M', 'Harbor City', 'CA') ,
    (23767, '35-45', 'M', 'Los Alamitos', 'CA') ,
    (18587, '25-35', 'M', 'South El Monte', 'CA') ,
    (43810, '18-25', 'F', 'Frazier Park', 'CA') ,
    (48721, '75-85', 'F', 'Los Angeles', 'CA') ,
    (36066, '55-65', 'M', 'Rosamond', 'CA') ,
    (13937, '35-45', 'M', 'Toluca Lake', 'CA') ,
    (36469, '85-95', 'M', 'Los Angeles', 'CA') ,
    (22501, '75-85', 'M', 'Duarte', 'CA') ,
    (36164, '85-95', 'F', 'Carson', 'CA')
    ''')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_user_lookup_table(self, schema_name):
    '''
    Creating the user_lookup table using the files from marketplace.
    '''
    
    ## CREATE THE user_lookup table
    print(f"Creating the user_lookup table within your Catalog's {schema_name} schema.")
    r = spark.sql(f'DROP TABLE IF EXISTS {self.catalog_name}.{schema_name}.user_lookup')

    r= spark.sql(f'''
    CREATE OR REPLACE TABLE {self.catalog_name}.{schema_name}.user_lookup AS
    SELECT 
        SHA2(CONCAT(user_id,'BEANS'), 256) as alt_id,
        device_id, 
        mac_address, 
        user_id
    FROM json.`/Volumes/dbacademy_gym_data/v01/user-reg`;
    ''')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_users_table(self, schema_name):
    '''
    Creating the users table in the specified schema within the user's unique catalog.
    '''

    print(f"Creating the users table within your Catalog's {schema_name} schema.")
        
    r = spark.sql(f'DROP TABLE IF EXISTS {self.catalog_name}.{schema_name}.users')

    r = spark.sql(f'''
    CREATE TABLE {self.catalog_name}.{schema_name}.users (
        alt_id STRING,
        updated TIMESTAMP,
        dob DATE,
        sex STRING,
        gender STRING,
        first_name STRING,
        last_name STRING,
        street_address STRING,
        city STRING,
        state STRING,
        zip INT,
        __Timestamp BIGINT,
        __DeleteVersion TIMESTAMP,
        __UpsertVersion TIMESTAMP
    )
    ''')


    r = spark.sql(f'''
    INSERT INTO {self.catalog_name}.{schema_name}.users (
        alt_id, updated, dob, sex, gender, first_name, last_name, 
        street_address, city, state, zip, __Timestamp, 
        __DeleteVersion, __UpsertVersion
    )
    VALUES
    ('0003f1837ee331d971fb1739220672ac5ba57ac7d974b18a747b54d114647da6', '2019-06-14T16:02:08Z', '1969-11-15', 'M', 'M', 'William', 'Gutierrez', '8954 Willie Club', 'Beverly Hills', 'CA', 90212.0, 1684480975187, null, '2019-06-14T16:02:08Z'),
    ('0a9c64223050770c9d2c720a803837ec28b001a437cd1d53f5fa6aafd0a5e002', '2019-06-14T19:14:08Z', '1978-08-27', 'M', 'M', 'Daniel', 'Ruiz', '2620 Hatfield Village Suite 039', 'Los Angeles', 'CA', 90073.0, 1684480975187, null, '2019-06-14T19:14:08Z'), ('0c2ae74ec7c5d1c07d300726ed6c55f1a5f01f505b215198b83721c7ca46fd11', '2019-07-09T16:55:28Z', '1930-01-25', 'M', 'M', 'Christopher', 'Kelly', '091 Eric Curve', 'Pacoima', 'CA', 91334.0, 1684480975187, null, '2019-07-09T16:55:28Z'), ('0fabdd33422fa07ecacd79b4c8d0799897a404b8eb52b23996c2b40cf3b600bc', '2019-07-29T04:50:08Z', '1987-06-11', 'F', 'F', 'Bonnie', 'Johnson', '4483 Samuel Route', 'Los Angeles', 'CA', 90047.0, 1684480975187, null, '2019-07-29T04:50:08Z'), ('1a19d87491f89887dd0dd3d79d7424247bfb3c5fc5ef5a632ca16c26f82c394c', '2019-06-11T03:05:36Z', '1954-01-17', 'M', 'M', 'William', 'Allen', '559 Katelyn Forks Apt. 417', 'Glendale', 'CA', 91208.0, 1684480975187, null, '2019-06-11T03:05:36Z'), ('1e403e6c7e8392701e6db4c225d8eb527c205fdd88e840e85b037ebc2c7a8cd0', '2019-08-12T13:05:04Z', '1933-12-13', 'M', 'M', 'Christopher', 'Juarez', '696 Justin Crest Apt. 656', 'El Monte', 'CA', 91731.0, 1684480975187, null, '2019-08-12T13:05:04Z'), ('2fd72bf6293f1d9a34e2cad33a78d01691d511c787151d1c6964b7f4321aa908', '2019-05-26T21:09:20Z', '1988-05-11', 'M', 'M', 'Brian', 'Williams', '27880 Dawn Lodge', 'Paramount', 'CA', 90723.0, 1684480975187, null, '2019-05-26T21:09:20Z'), ('54c9a8be55ad6b77cc376ef3638fc4af1ae1961685edd4cd986a5376c46bc67f', '2019-07-04T00:57:36Z', '1972-03-30', 'M', 'M', 'Cameron', 'Vasquez', '95932 Gary Ridges', 'Los Angeles', 'CA', 90018.0, 1684480975187, null, '2019-07-04T00:57:36Z'), ('7fde9776f2be9726369921eb480568b3e726110af056b36fe7a3ffdf4f9f6b79', '2019-08-10T22:06:56Z', '1925-02-26', 'M', 'M', 'Joshua', 'Perry', '3456 Snyder Coves', 'Torrance', 'CA', 90501.0, 1684480975187, null, '2019-08-10T22:06:56Z'), ('8846b301bd5228b145e8ed3ec3bf809e2f1d27ccd77ca838fe49c305665733e4', '2019-07-18T18:25:04Z', '1964-01-05', 'F', 'F', 'Sandra', 'Flores', '203 Nicholson Mountains', 'Glendale', 'CA', 91201.0, 1684480975187, null, '2019-07-18T18:25:04Z'), ('886f1bdac99a41378ec91389e91c1f4d8033e5cd61084223d9ca95807d812a55', '2019-07-28T10:20:48Z', '1995-08-08', 'M', 'M', 'Justin', 'Richardson', '42921 Patrick Parkway', 'Los Angeles', 'CA', 90009.0, 1684480975187, null, '2019-07-28T10:20:48Z'), ('8a5876de067c1386bdb74084a16d2c7f2f18fbdee589ffe73c0ceb4f38eb5922', '2019-04-17T22:19:44Z', '2001-02-01', 'F', 'F', 'Shelley', 'Andrews', '4791 Nathan Turnpike Suite 540', 'Los Angeles', 'CA', 90024.0, 1684480975187, null, '2019-04-17T22:19:44Z'), ('8c7a958028240ea16e7368fbca88f3b9846e8a6e059586fda5d5b0c7863b94aa', '2019-12-01T04:56:32Z', '1995-10-07', 'M', 'M', 'Austin', 'Medina', '3003 Selena Dale', 'Lakewood', 'CA', 90713.0, 1684480975187, null, '2019-12-01T04:56:32Z'), ('8e53b301a9819419b49d4315fe9c95fd752e2d72a94820f89f1acd4d14eb560f', '2019-06-04T22:51:44Z', '1965-05-10', 'F', 'F', 'Jennifer', 'Perez', '973 Norman Mountain Apt. 290', 'Canyon Country', 'CA', 91386.0, 1684480975187, null, '2019-06-04T22:51:44Z'), ('9ac4a92a8ad072a8c613708fa3dada52f430ead19466922536403464f16c5f48', '2019-06-14T10:14:24Z', '1937-03-27', 'M', 'M', 'Darrell', 'Gonzalez', '1945 George Inlet Suite 490', 'Valyermo', 'CA', 93563.0, 1684480975187, null, '2019-06-14T10:14:24Z'), ('9b6a45bc85eec4a82dcdcef021834b0d221a54c977541e871f5e208b6234261e', '2019-12-01T03:24:48Z', '1957-11-08', 'F', 'F', 'Courtney', 'Harris', '4634 Nicholas Mission Apt. 388', 'Playa Vista', 'CA', 90094.0, 1684480975187, null, '2019-12-01T03:24:48Z'), ('a6f9678bccb4d7e7bc0ba5afbe9ba83c154889506173f51c1d55bc19c115bec6', '2019-07-27T21:26:24Z', '1936-01-28', 'M', 'M', 'Justin', 'Eaton', '04952 Lori Plain', 'Sierra Madre', 'CA', 91024.0, 1684480975187, null, '2019-07-27T21:26:24Z'), ('ac2bdd6fb77613c084683bebdb9bf5f131418174a7b705b9d90e360d34eb79bf', '2019-06-12T22:43:12Z', '1991-04-18', 'M', 'M', 'Kevin', 'Phillips', '60685 Pena Crossroad', 'Santa Monica', 'CA', 90403.0, 1684480975187, null, '2019-06-12T22:43:12Z'), ('b6f66a657af52e7490a825eed37cbb9894064e04f29d419f7f1f786b98046391', '2019-07-12T06:26:08Z', '1991-11-07', 'F', 'F', 'Tammy', 'Barry', '70678 Oliver Ways', 'Santa Clarita', 'CA', 91350.0, 1684480975187, null, '2019-07-12T06:26:08Z'), ('bf89f14b9b61eb3369399515152f55c944de897791ff42e5d1ef9305ad2da394', '2019-07-01T19:52:32Z', '1927-03-10', 'F', 'F', 'Audrey', 'Hall', '8170 Marcus Course Suite 766', 'North Hollywood', 'CA', 91618.0, 1684480975187, null, '2019-07-01T19:52:32Z'), ('c1d481a318d47d83198de1e96308341f9ef6f5a47292dccfdc000d4eefa9f3f6', '2019-05-06T23:08:48Z', '1958-07-16', 'F', 'F', 'Stephanie', 'Martinez', '1767 Katherine Expressway Suite 062', 'Edwards', 'CA', 93523.0, 1684480975187, null, '2019-05-06T23:08:48Z'), ('c65fc3b744666216a6d3f3d23686cbcd95c823368c132f05314d9ecac587b170', '2019-07-22T21:49:52Z', '1964-11-25', 'M', 'M', 'William', 'Nichols', '1289 Mark Union Suite 964', 'Los Angeles', 'CA', 90011.0, 1684480975187, null, '2019-07-22T21:49:52Z'), ('c7d80aeb1a9e7653dc13dadec6075e3e2a32f0226bfeac1968248b4c7164d8ab', '2019-05-16T07:00:16Z', '1944-11-19', 'M', 'M', 'Joshua', 'Mitchell', '843 Hannah Corners', 'Woodland Hills', 'CA', 91367.0, 1684480975187, null, '2019-05-16T07:00:16Z'), ('d5671b518173cf5bf1dbdf634bb500984acf040bd5250c944bb46b848c601b85', '2019-12-01T17:59:28Z', '1944-07-22', 'F', 'F', 'Alexis', 'Hicks', '50780 Brooks Ports Suite 164', 'Studio City', 'CA', 91614.0, 1684480975187, null, '2019-12-01T17:59:28Z'), ('e6a6387ffbb75e14eb39b138be1e7e9b8a6687e66c36865bf6e234d6688dd517', '2019-12-01T05:28:32Z', '1939-07-25', 'M', 'M', 'Matthew', 'Phillips', '02648 Wilkins Cliffs Suite 998', 'San Fernulldo', 'CA', 91340.0, 1684480975187, null, '2019-12-01T05:28:32Z'), ('f067aca130864b28c4413ae218f6e8101c8ea68062e7c5a8d0d2639d0a3a8564', '2019-05-31T11:54:40Z', '1987-12-08', 'F', 'F', 'Debbie', 'Ramirez', '773 Rice Wall', 'Carson', 'CA', 90895.0, 1684480975187, null, '2019-05-31T11:54:40Z'), ('fad7615e297311025a0abe1b742aed061427ef01f8942c6e029cb2fc4ad887e6', '2019-12-01T11:31:12Z', '1982-04-23', 'M', 'M', 'Matthew', 'Johnson', '18310 Bowers Gardens', 'Arcadia', 'CA', 91006.0, 1684480975187, null, '2019-12-01T11:31:12Z'), ('096d49b07b21d1443ad4a835a87c834fa284468d72ffb87e730dcf734db5abd4', '2019-12-08T01:40:16Z', '1949-09-06', 'F', 'F', 'Kelly', 'Hardy', '28033 Anita Brooks', 'Rowland Heights', 'CA', 91748.0, 1684481179574, null, '2019-12-08T01:40:16Z'), ('0cac08c9ce7ea9534718f6f1047ed792015f9dc5aecc2e121e4c80c68ae7bb64', '2019-12-15T08:34:08Z', '1984-01-18', 'F', 'F', 'Leah', 'Walker', '6973 Conner Viaduct', 'Burbank', 'CA', 91521.0, 1684481179574, null, '2019-12-15T08:34:08Z'), ('0d26f72ab63e5564c97a3c9d8eb77b22ffb1e1bde0ecd5781af4cf79d537be33', '2019-12-15T12:20:16Z', '1996-11-21', 'F', 'F', 'Amanda', 'Sanchez', '964 Nathan Dale Suite 191', 'Sierra Madre', 'CA', 91024.0, 1684481179574, null, '2019-12-15T12:20:16Z'), ('14b6180e0982573e5faf0c426cd9862075f28f02274ef5db780d9a3683ac6974', '2019-12-04T17:10:24Z', '1928-03-17', 'M', 'M', 'Michael', 'Johnston', '209 Rodriguez Shoal Suite 111', 'Signal Hill', 'CA', 90755.0, 1684481179574, null, '2019-12-04T17:10:24Z'), ('1cf68d590ac4280b5876867d64061463b0656c85c81afe90938f87f5ecf1e0a6', '2019-12-15T14:58:08Z', '1980-07-29', 'M', 'M', 'David', 'Woodard', '0711 David Summit Apt. 963', 'Woodland Hills', 'CA', 91367.0, 1684481179574, null, '2019-12-15T14:58:08Z'), ('22cd18aa025b6e0b792b992b8ce7573c1a29a651cc72ed158c80daabbe660101', '2019-12-16T16:38:24Z', null, null, null, null, null, null, null, null, null, 1684481179574, '2019-12-16T16:38:24Z', '2019-12-16T16:38:24Z'), ('2af970fb2c6d514d11c4738f07306469b05681eb57817b1ad1566ce85a7676e1', '2019-12-15T04:22:24Z', '1936-09-06', 'M', 'M', 'Craig', 'Nunez', '882 Pamela Stravenue Suite 687', 'Los Alamitos', 'CA', 90720.0, 1684481179574, null, '2019-12-15T04:22:24Z'), ('2e8ec9f76a8bf5e4d860e7c994983bfa7a2facf23a511440837cb716b5fb2b20', '2019-12-06T11:20:32Z', '1979-07-21', 'M', 'M', 'Scott', 'Carr', '4590 Andrew Shore Apt. 004', 'Inglewood', 'CA', 90302.0, 1684481179574, null, '2019-12-06T11:20:32Z'), ('2f6d904bc5175d372a81c8a2f2931550cc1df2330b62216739f9e9fddfa4ec79', '2019-12-09T09:16:48Z', '1930-03-01', 'M', 'M', 'Andrew', 'Monroe', '5605 Walsh Orchard', 'Glendale', 'CA', 91205.0, 1684481179574, null, '2019-12-09T09:16:48Z'), ('39e5d7cc7a7354cab35d1ae9b4395e2303337d4a37b26f56071462c3a39b9b95', '2019-12-09T01:10:24Z', null, null, null, null, null, null, null, null, null, 1684481179574, '2019-12-09T01:10:24Z', '2019-12-09T01:10:24Z'), ('3de9b9bd48d773210bb42f9b566665356e73e1b35ae5584eec5824d57042fc89', '2019-12-06T18:33:36Z', '1949-03-28', 'M', 'M', 'Joseph', 'Aguilar', '80176 Jeremy Harbor Apt. 565', 'Los Angeles', 'CA', 90033.0, 1684481179574, null, '2019-12-06T18:33:36Z'), ('43a9a48524fca3e47fd42d2669013c49c567cedeeb506d1fd7a751f75f1c8687', '2019-12-12T04:05:20Z', '1952-07-04', 'M', 'M', 'Joseph', 'Kane', '56860 Fernulldo Lock Apt. 520', 'Glendale', 'CA', 91204.0, 1684481179574, null, '2019-12-12T04:05:20Z'), ('43e92394e04e05a79a6db09806d0cd800a08f03cefc429c31c800b2aeb33caa3', '2019-12-09T15:49:20Z', '1982-03-08', 'M', 'M', 'David', 'King', '959 Snyder Extensions', 'Los Angeles', 'CA', 90038.0, 1684481179574, null, '2019-12-09T15:49:20Z'), ('445d111a53d2f69b1ea747c8478680a76544bfaa571ca6a144d999bd664f7b30', '2019-12-11T11:37:36Z', '1925-03-01', 'M', 'M', 'Christopher', 'Phelps', '7342 Sarah Cliffs Apt. 262', 'Arcadia', 'CA', 91007.0, 1684481179574, null, '2019-12-11T11:37:36Z'), ('48413178107e7ce80cd8ad7ddb7b50a2ef9b8d9109e0391f79ce914e9d1d14d6', '2019-12-09T15:23:44Z', '1986-04-28', 'F', 'F', 'Sandra', 'Everett', '200 Trevino Junction', 'Northridge', 'CA', 91330.0, 1684481179574, null, '2019-12-09T15:23:44Z'), ('4a63161d94e3318c38193842d17054d239dc9b9261ea1cbac66de86f30285214', '2019-12-02T12:35:12Z', '1965-10-30', 'M', 'M', 'Gary', 'Perry', '72915 Ross Throughway', 'Rowland Heights', 'CA', 91748.0, 1684481179574, null, '2019-12-02T12:35:12Z'), ('52596bda400a4f5a871731a141b0a073c05789cd3f18abf4d418c9d8415a64e0', '2019-12-15T01:04:00Z', '1932-01-04', 'F', 'F', 'Katie', 'Gonzalez', '6381 Davis Light', 'Santa Clarita', 'CA', 91350.0, 1684481179574, null, '2019-12-15T01:04:00Z'), ('56b4e728296213329ae705e16b91f5aaaa07cf3c6bc116cced4e3c6bc0f5fd1e', '2019-12-05T02:33:36Z', '1960-10-28', 'M', 'M', 'Robert', 'Mccormick', '3694 Warner Ville', 'Los Angeles', 'CA', 90027.0, 1684481179574, null, '2019-12-05T02:33:36Z'), ('57098cb66f100853a15a02d5c48aed34954298c0f6b8a974937edd861a8b1373', '2019-12-14T08:12:48Z', '1999-05-04', 'F', 'F', 'Lisa', 'Jensen', '7175 Forbes Union', 'Pacoima', 'CA', 91331.0, 1684481179574, null, '2019-12-14T08:12:48Z'), ('5bca0b934b0807a31f51e1b93c2ba2932e34c4ae29eb972b2c0b94c9047d992b', '2019-12-05T01:59:28Z', '1941-11-22', 'F', 'F', 'Kathryn', 'Campos', '962 Ronald Isle', 'Calabasas', 'CA', 91302.0, 1684481179574, null, '2019-12-05T01:59:28Z'), ('5cb9d8c61cdf59147530b03a67cce1b72a103657a35be61b5ca293473e76fa0d', '2019-12-02T06:17:36Z', '1928-09-08', 'M', 'M', 'Jose', 'Barry', '5454 Perez Crossroad Suite 544', 'Long Beach', 'CA', 90822.0, 1684481179574, null, '2019-12-02T06:17:36Z'), ('5ce6620b7c357cd69446a87f463a4c40b100364e706a661dcec12390b9852d53', '2019-12-13T09:36:00Z', null, null, null, null, null, null, null, null, null, 1684481179574, '2019-12-13T09:36:00Z', '2019-12-13T09:36:00Z'), ('5d78cff0ca0f659e86ef1118bbf5b322731890ac20e0079fa799c44c80f1feff', '2019-12-08T23:21:36Z', null, null, null, null, null, null, null, null, null, 1684481179574, '2019-12-08T23:21:36Z', '2019-12-08T23:21:36Z'), ('5f0434799f564be731a879a912567b22a02de0a6a8d6a78d251a3f1999588fad', '2019-12-10T02:18:40Z', '1930-09-14', 'M', 'M', 'Brian', 'Wood', '42172 Ross Forge', 'Alhambra', 'CA', 91803.0, 1684481179574, null, '2019-12-10T02:18:40Z'), ('6ae2eb91031e5980c271eff64aeea2bf6c0127f380c0f93dd5077228c562bd61', '2019-12-03T21:22:08Z', '1967-11-13', 'M', 'M', 'Richard', 'Douglas', '993 Smith Mountain Apt. 056', 'Pomona', 'CA', 91768.0, 1684481179574, null, '2019-12-03T21:22:08Z'), ('6b46105aab33210a8430dfc605c034c9f14b7a5bdc53023f2196c214762b3921', '2019-12-02T10:01:36Z', '1935-08-07', 'M', 'M', 'Jason', 'Miller', '57759 Kayla Glen Suite 220', 'Newhall', 'CA', 91321.0, 1684481179574, null, '2019-12-02T10:01:36Z'), ('6ba11fc3885526ddba3b6bda07d5826307b7083e5451ea6f091ebd8ebb5df9f8', '2019-12-09T18:54:56Z', '1964-06-28', 'F', 'F', 'Angela', 'Ford', '931 Jill Terrace Suite 404', 'Montebello', 'CA', 90640.0, 1684481179574, null, '2019-12-09T18:54:56Z'), ('6ff4ee52ca804210187ce49a76e3ea347e02593ab47134ac0deec8598bdde6a8', '2019-12-04T06:24:00Z', '1976-11-13', 'F', 'F', 'Sarah', 'Olson', '2487 Christina Estate Apt. 556', 'Long Beach', 'CA', 90803.0, 1684481179574, null, '2019-12-04T06:24:00Z'), ('71324d786c7f10bd56edf127946a934016c65ff515c47f4d6ea5d233d5cae66a', '2019-12-09T09:31:44Z', '1979-01-04', 'M', 'M', 'Edward', 'Smith', '41444 Noble Cape Suite 390', 'North Hollywood', 'CA', 91606.0, 1684481179574, null, '2019-12-09T09:31:44Z'), ('73001aebb56c459e295c092d9f5c31355eb462fbd100c670fa48c67510a302ab', '2019-12-10T13:30:40Z', '1992-01-02', 'M', 'M', 'Christopher', 'Frey', '76662 Thompson Ville', 'Northridge', 'CA', 91327.0, 1684481179574, null, '2019-12-10T13:30:40Z'), ('745c35775cfdf69e9a190c26a173285712c1929c5faa5087ce2855e9043b33f4', '2019-12-08T00:06:24Z', '1955-06-29', 'F', 'F', 'Shannon', 'Reyes', '3105 Bowers Expressway', 'Long Beach', 'CA', 90808.0, 1684481179574, null, '2019-12-08T00:06:24Z'), ('7e0504eb0e145b999b39081c43ac2cc7f26ec9ba8e4b06d5337be256c28a72ba', '2019-12-12T20:58:40Z', '1944-08-13', 'M', 'M', 'Lee', 'Brown', '81022 Gibson Fords', 'Palmdale', 'CA', 93552.0, 1684481179574, null, '2019-12-12T20:58:40Z'), ('7f704b7b0053aef6720144e0c32bf9481894b39c16607c23c998906ed28581d7', '2019-12-03T03:22:40Z', '1979-04-29', 'M', 'M', 'Glenn', 'Anderson', '95880 Vargas Station', 'Redondo Beach', 'CA', 90277.0, 1684481179574, null, '2019-12-03T03:22:40Z'), ('858cf05d9f2a9db98eb92ebe1575a62fc2565a1ff0e7b7f458f239e9d738a790', '2019-12-09T16:59:44Z', '1989-04-23', 'M', 'M', 'William', 'Hinton', '000 Huber Extension Apt. 632', 'Pacoima', 'CA', 91334.0, 1684481179574, null, '2019-12-09T16:59:44Z'), ('898fa1b7f7057b1b17bca1342b4341b7d84e228d6d9bb83a7a770c4d487ebc4b', '2019-12-10T21:30:40Z', '1980-03-06', 'F', 'F', 'Victoria', 'Smith', '634 Acevedo Mountain', 'Santa Monica', 'CA', 90405.0, 1684481179574, null, '2019-12-10T21:30:40Z'), ('8bb09f205d023251e392804a20a7294f5a424c9c295054c8f8f293c76dd205a5', '2019-12-15T10:46:24Z', null, null, null, null, null, null, null, null, null, 1684481179574, '2019-12-15T10:46:24Z', '2019-12-15T10:46:24Z'), ('8bdbabab0376e9bc29748fd75ba8b669e9947dbcb0ee492a32a4d5c1b604321f', '2019-12-11T20:54:24Z', '1997-09-04', 'F', 'F', 'Hannah', 'Fuller', '81346 Obrien Streets', 'Gardena', 'CA', 90249.0, 1684481179574, null, '2019-12-11T20:54:24Z'), ('8ce057164dbf8cd11eb931128eb30707cecde30791620893f4f80f9a31bd611b', '2019-12-07T18:01:36Z', '1960-01-18', 'F', 'F', 'Gail', 'Bush', '89522 Reyes Knoll', 'Canyon Country', 'CA', 91386.0, 1684481179574, null, '2019-12-07T18:01:36Z'), ('8eb9a08411fac3e94fc6ca6daadc6063355653c99d9556d4956fb6b6960d3125', '2019-12-14T17:48:48Z', '1931-09-20', 'F', 'F', 'Priscilla', 'Cruz', '92119 Lewis Valley', 'Playa Vista', 'CA', 90094.0, 1684481179574, null, '2019-12-14T17:48:48Z'), ('947c62862fee6f044848b06b376e3e0555f6fdac746456a28dd011a964d6ae57', '2019-12-11T14:17:36Z', '1995-10-20', 'F', 'F', 'Erika', 'Lee', '308 Jennifer Village', 'Long Beach', 'CA', 90805.0, 1684481179574, null, '2019-12-11T14:17:36Z'), ('96907ad85a4bc7fc3245f1ca29b4fe7cf0d4c61ece6ec385382e9ff164fb5b51', '2019-12-08T22:53:52Z', '1942-03-07', 'F', 'F', 'Joanna', 'Wheeler', '740 Pollard Hill Apt. 995', 'Covina', 'CA', 91723.0, 1684481179574, null, '2019-12-08T22:53:52Z'), ('a2ce1436d4cd2e4376dfa1afe92140148835e6e19a6b523c611f2af6e6d48b28', '2019-12-15T19:33:20Z', '1972-02-12', 'F', 'F', 'Megan', 'Daugherty', '5641 Kelly Tunnel Apt. 584', 'Los Angeles', 'CA', 90026.0, 1684481179574, null, '2019-12-15T19:33:20Z'), ('a413776c0b52be648193169808a9da068a8c502f9de1c6cbbe4d0624e916aa24', '2019-12-09T09:42:24Z', '1946-11-05', 'M', 'M', 'Chris', 'Wright', '5700 Catherine Fort Suite 808', 'Culver City', 'CA', 90232.0, 1684481179574, null, '2019-12-09T09:42:24Z'), ('a610459add89c3c6a1de87bf8beaa4aee317564837e8d0247852004197e7d8cc', '2019-12-12T08:04:16Z', '1999-02-02', 'M', 'M', 'Robert', 'Castillo', '68994 Steven Vista', 'Pearblossom', 'CA', 93553.0, 1684481179574, null, '2019-12-12T08:04:16Z'), ('ac18a7a9aea94ea019452727c55204c949a6055f3f0ebba3852260f9bab729bd', '2019-12-05T03:39:44Z', '2001-12-20', 'F', 'F', 'Elizabeth', 'Stone', '613 Duffy River Suite 641', 'Edwards', 'CA', 93523.0, 1684481179574, null, '2019-12-05T03:39:44Z'), ('ac361189be33b056bf0d742ebc33410179d985168574c629f0ffdf0c2a54c4b7', '2019-12-10T14:43:12Z', '1973-01-12', 'M', 'M', 'Karl', 'Scott', '50078 Ryan Springs Apt. 646', 'Long Beach', 'CA', 90840.0, 1684481179574, null, '2019-12-10T14:43:12Z'), ('ad1777c323866c48b547546d84d0d49d54c160467db7b25baebee9f6aa30438e', '2019-12-09T01:55:12Z', '1940-11-21', 'M', 'M', 'Nicholas', 'Khan', '418 Johnson Cape Suite 549', 'Santa Clarita', 'CA', 91382.0, 1684481179574, null, '2019-12-09T01:55:12Z'), ('b0b1ca4d901b4a8acbb7bca88398a25c93a78f2d134abf10d78fed4a8d109d89', '2019-12-02T05:32:48Z', '1940-01-20', 'F', 'F', 'Wendy', 'Wheeler', '9318 Spencer Extensions Suite 442', 'Beverly Hills', 'CA', 90211.0, 1684481179574, null, '2019-12-02T05:32:48Z'), ('b3ce898d04bce048857c8dccc8bd3b437115c01a7dbdcdb97f738829a1785744', '2019-12-08T09:23:12Z', '1944-08-23', 'F', 'F', 'Pamela', 'Wilson', '009 William Trace', 'Beverly Hills', 'CA', 90210.0, 1684481179574, null, '2019-12-08T09:23:12Z'), ('b546a0d05fa1c47aa93fe2f3b7091d5100803bd318d3c8b077e3f3cc475cdf5b', '2019-12-08T09:10:24Z', '1951-11-12', 'M', 'M', 'Paul', 'Hinton', '17423 Fox Highway Apt. 163', 'Pinon Hills', 'CA', 92372.0, 1684481179574, null, '2019-12-08T09:10:24Z'), ('b7f89b0222f9d2f41413ab2570fb32aa8b2029f3c2a6280adc85efca33ddc266', '2019-12-11T06:09:04Z', '1929-10-20', 'F', 'F', 'Jennifer', 'Cardenas', '541 Kelly Rue Apt. 638', 'Los Angeles', 'CA', 90038.0, 1684481179574, null, '2019-12-11T06:09:04Z'), ('b8056cd4844bd4b278606f71e40545c1b93d5e8379207ee705ef560bc07158a3', '2019-12-13T17:08:16Z', '1980-09-10', 'F', 'F', 'Angelica', 'Davis', '2665 Richard Cliff', 'Glendale', 'CA', 91205.0, 1684481179574, null, '2019-12-13T17:08:16Z'), ('b945ade887897dab37e750fe7e77af5045b2ac88d8053ebc89566939f0ca4611', '2019-12-15T23:13:04Z', '1981-01-05', 'M', 'M', 'Patrick', 'Hunt', '7768 Dawn Skyway', 'South Gate', 'CA', 90280.0, 1684481179574, null, '2019-12-15T23:13:04Z'), ('ba4f45d7a2d2df344cfdeecf9d2417eb6cf4a97ffc7f03d0acfdec510ebf4c37', '2019-12-11T10:27:12Z', '1994-10-12', 'F', 'F', 'Samantha', 'Jones', '323 Ryan Mission', 'Carson', 'CA', 90749.0, 1684481179574, null, '2019-12-11T10:27:12Z'), ('c19bc3c6b4d8028f92fefac5feb50e9ed0ad96dfe1e5eec20669a892c9bc5689', '2019-12-03T07:08:48Z', '1990-10-04', 'M', 'M', 'Robert', 'Campbell', '53722 Hardy Expressway Apt. 308', 'La Crescenta', 'CA', 91224.0, 1684481179574, null, '2019-12-03T07:08:48Z'), ('c3274e338a1da85e0748f6c9c92247e176044770f4e39637a4c4d4b348191fcf', '2019-12-08T14:56:00Z', '1949-12-11', 'F', 'F', 'Courtney', 'Sheppard', '47754 Angela Plaza Apt. 135', 'Los Angeles', 'CA', 90010.0, 1684481179574, null, '2019-12-08T14:56:00Z'), ('c375e4b6624769bc9810362d8d615174eaa532b8818c093934af8e088b93352a', '2019-12-11T06:49:36Z', '1972-08-15', 'M', 'M', 'Samuel', 'Riddle', '34060 Jennings Parkways', 'Los Angeles', 'CA', 90061.0, 1684481179574, null, '2019-12-11T06:49:36Z'), ('c495b47a54d95311645cef71d286c41106fb6c364c00fb085e4f70fe9dd02779', '2019-12-07T14:02:40Z', '1937-01-16', 'F', 'F', 'Elizabeth', 'Guzman', '942 Vanessa Oval Suite 444', 'Los Angeles', 'CA', 90022.0, 1684481179574, null, '2019-12-07T14:02:40Z'), ('c8af87ecdf5b41e60f46d85ee24c8189bd643a4540d26c59d3ee5c36098fa4de', '2019-12-09T09:21:04Z', '1972-11-10', 'F', 'F', 'Mary', 'Gibson', '8795 Michael Point', 'Los Angeles', 'CA', 90040.0, 1684481179574, null, '2019-12-09T09:21:04Z'), ('ca426039ab08642d25ef448bad89172dc6271366c4137c3003fe32d1cb300c44', '2019-12-10T09:40:16Z', '1941-03-07', 'M', 'M', 'Casey', 'Johnson', '27548 Craig Dale Apt. 118', 'Altadena', 'CA', 91001.0, 1684481179574, null, '2019-12-10T09:40:16Z'), ('caae89a762fd88b148a287292b5684963207dc5a737c3fa0756e346434f90aea', '2019-12-09T14:34:40Z', '1951-12-23', 'M', 'M', 'Carlos', 'Schneider', '665 Laura Brooks Apt. 958', 'Monrovia', 'CA', 91016.0, 1684481179574, null, '2019-12-09T14:34:40Z'), ('cac9d03ade3b1054e30819a6a761118dfc7fb52d2b55a9b8b5877f628a6a1401', '2019-12-12T13:09:20Z', '1928-09-24', 'M', 'M', 'Timothy', 'Haynes', '9698 Gerald Pines', 'Avalon', 'CA', 90704.0, 1684481179574, null, '2019-12-12T13:09:20Z'), ('cba874842f4dd76d361c730e6c6080f130d86f149c1f6faeaf3d0dc605c85d17', '2019-12-16T00:12:48Z', null, null, null, null, null, null, null, null, null, 1684481179574, '2019-12-16T00:12:48Z', '2019-12-16T00:12:48Z'), ('cd887f4d983dde497360264c66a28960b6f8d35fccc87aef39f661fb61f6b158', '2019-12-14T20:05:20Z', '1930-01-28', 'M', 'M', 'Tyler', 'Medina', '74229 Julie Via', 'Harbor City', 'CA', 90710.0, 1684481179574, null, '2019-12-14T20:05:20Z'), ('d61f83cf21c535545b2b679f5c5f483e395d146c3a1f22e5c6705a1ceb5cc9c3', '2019-12-06T03:48:16Z', '1979-06-01', 'M', 'M', 'Daniel', 'Brown', '1954 Rivera Shoal Apt. 919', 'Los Alamitos', 'CA', 90720.0, 1684481179574, null, '2019-12-06T03:48:16Z'), ('d69601f8b2daa4176fa1d262de48bd4d54a7d5ca0e5ca121a779cd0a3f2cff8b', '2019-12-03T15:51:28Z', '1991-09-24', 'M', 'M', 'Stephen', 'Gray', '83946 William Village Apt. 825', 'South El Monte', 'CA', 91733.0, 1684481179574, null, '2019-12-03T15:51:28Z'), ('debb243bbdd4fc9a7d740ba5f5d9084b8cba6f77735f5113401c9b747ca1ab17', '2019-12-14T15:38:40Z', '2000-09-16', 'F', 'F', 'Ana', 'Williams', '12665 Debra Crossing', 'Frazier Park', 'CA', 93225.0, 1684481179574, null, '2019-12-14T15:38:40Z'), ('e4ec8f3c228562d1720082e76bce797f1861386d6ea07e4cf616d3e3343412dc', '2019-12-09T16:25:36Z', '1943-05-02', 'F', 'F', 'Mary', 'Miles', '522 Marshall Well Suite 853', 'Los Angeles', 'CA', 90068.0, 1684481179574, null, '2019-12-09T16:25:36Z'), ('e5355dd0856aaf1b71cef63a62613d72f58467e0e102d871e4b700323a59b8f7', '2019-12-09T07:36:32Z', '1965-08-26', 'M', 'M', 'Kurt', 'King', '464 Gregory Drives', 'Rosamond', 'CA', 93560.0, 1684481179574, null, '2019-12-09T07:36:32Z'), ('e83442f287db35be0374a66e2253ab5c119417b0e559554291ac2af8f7f5809f', '2019-12-03T19:03:28Z', '1982-04-26', 'M', 'M', 'Matthew', 'Johnson', '9231 Edward Throughway Suite 072', 'Toluca Lake', 'CA', 91610.0, 1684481179574, null, '2019-12-03T19:03:28Z'), ('e8a0581852fadf7d7527dd345fbc34b828f30019915f042648999e54175339cc', '2019-12-14T00:23:28Z', '1933-07-13', 'M', 'M', 'Nicholas', 'James', '987 Marshall Oval Apt. 675', 'Los Angeles', 'CA', 90017.0, 1684481179574, null, '2019-12-14T00:23:28Z'), ('ee806d2054496010b55c1be3a1669358e528faa98eb4c96325683335eb053e77', '2019-12-11T02:10:08Z', '1945-09-13', 'M', 'M', 'Nicholas', 'Wagner', '8360 Natasha Camp', 'Duarte', 'CA', 91008.0, 1684481179574, null, '2019-12-11T02:10:08Z'), ('f77a5f0c87d8c4114be62db6adef9d88d1979c5ad5cd43205667540939928d3f', '2019-12-13T19:18:24Z', '1935-10-08', 'F', 'F', 'Caitlin', 'Miles', '176 Sandra Lane Suite 564', 'Carson', 'CA', 90746.0, 1684481179574, null, '2019-12-13T19:18:24Z')
    ''')