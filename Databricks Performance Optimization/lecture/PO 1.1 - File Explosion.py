# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # File Explosion
# MAGIC We see many data engineers partitioning their tables in ways that can cause major performance issues, without improving future query performance. This is called "over partitioning". We'll see what that looks like in practice in this demo.
# MAGIC
# MAGIC ##### Useful References
# MAGIC - [Partitioning Recommendations](https://docs.databricks.com/en/tables/partitions.html)
# MAGIC - [CREATE TABLE Syntax](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)
# MAGIC - [About ZORDER](https://docs.databricks.com/en/delta/data-skipping.html)
# MAGIC - [About Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default. If you use Serverless, errors will be returned when setting compute runtime properties.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique **labuser** catalog, and the default schema to **default**. All tables will be read from and written to this location.
# MAGIC <br></br>
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC Let's Check the Current Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Disable Caching
# MAGIC
# MAGIC Run the following cell to set a Spark configuration variable that disables disk caching.
# MAGIC
# MAGIC Turning disk caching off prevents Databricks from storing cloud storage files after the first query. This makes the effect of the optimizations more apparent by ensuring that files are always pulled from cloud storage for each query.
# MAGIC
# MAGIC For more information, see [Optimize performance with caching on Databricks](https://docs.databricks.com/en/optimizations/disk-cache.html#optimize-performance-with-caching-on-databricks).
# MAGIC
# MAGIC **NOTE:** This will not work in Serverless. Please use classic compute to turn off caching. If you're using Serverless, an error will be returned.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## C. Process & Write IoT data
# MAGIC Let's generate some fake IoT data. This first time around, we are only going to generate 2,500 rows.

# COMMAND ----------

from pyspark.sql.functions import *

df = (spark
      .range(0, 2500)
      .select(
          hash('id').alias('id'), # randomize our ids a bit
          rand().alias('value'),
          from_unixtime(lit(1701692381 + col('id'))).alias('time') 
      ))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll write the data to a table partitioned by **id** (2,500 distinct values), which will result in every row being written to a separate folder for each partition. Writing 2,500 rows in this fashion will take a long time, as we are creating 2,500 partitions. Each partition will contain a folder with one file, and each file will store one row of data for each **id**, leading to the 'small file' problem.
# MAGIC
# MAGIC Note how long it takes to generate the table.
# MAGIC
# MAGIC **NOTE:** This will take about 1-2 minutes to create the table with 2,500 partitions.
# MAGIC

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS iot_data_partitioned')

(df
 .write
 .mode('overwrite')
 .option("overwriteSchema", "true")
 .partitionBy('id')
 .saveAsTable("iot_data_partitioned")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the history of the **iot_data_partitioned** table. Confirm the following:
# MAGIC - In the **operationParameters** column, the table is partitioned by **id**.
# MAGIC - In the **operationMetrics** column, the table contains 2,500 files, one parquet file for each unique partitioned **id**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_data_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC You can use the `SHOW PARTITIONS` statement to list all partitions of a table. Run the code and view the results. Notice that the table is partitioned by **id** and contains 2,500 rows.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS iot_data_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Query the Table
# MAGIC Run the two queries against the partitioned table we just created. Note the time taken to execute each query.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 1 - Filter by the partitioned id column
# MAGIC **NOTE:** (1-2 second execution)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: Filtering by the partitioned column. 
# MAGIC SELECT * 
# MAGIC FROM iot_data_partitioned 
# MAGIC WHERE id = 519220707;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
# MAGIC
# MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
# MAGIC
# MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
# MAGIC
# MAGIC 4. Here you should see the entire query plan.
# MAGIC
# MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy.*your schema*.iot_data_partitioned (1)** and select the plus icon.
# MAGIC
# MAGIC
# MAGIC ![1.1-query-1-iot_partitioned.png](./Includes/images/1.1-query-1-iot_partitioned.png)
# MAGIC
# MAGIC #### Look at the following metrics in the Spark UI:
# MAGIC
# MAGIC
# MAGIC | Metric    | Value    | Note    |
# MAGIC |-------------|-------------|-------------|
# MAGIC | cloud storage request count| 1| Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data. <br></br>Monitoring this metric helps optimize performance, reduce costs, and identify potential inefficiencies in data access patterns. |
# MAGIC | cloud storage response size| 880.0B|  Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
# MAGIC | **files pruned** | **2,499** |Indicates the number of files that Spark skipped or ignored during a job execution. A total of 2,499 files were skipped by Spark due to pruning based on the query filtering by **id**. This is due to the table being partitioned by **id**, the queried column. Spark reads only the necessary partitions for processing and skips the other partitions.|
# MAGIC | **files read** | **1**|  Indicates the number of files that Spark has actually read during job execution. Here, 1 file was read during the execution of the Spark job. Only 1 file was read because the query was executed on the partitioned **id** column. Spark only needs to read the necessary partitions(s) based on the query.
# MAGIC
# MAGIC #### Summary
# MAGIC Because the data was partitioned by **id** and queried by the partitioned column, Spark will only read the necessary partition(s) (one partition in this example) and will skip the other partitioned files.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2 - Filter by a column that is not partitioned 
# MAGIC **NOTE:** (5-10 second execution)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(value) 
# MAGIC FROM iot_data_partitioned 
# MAGIC WHERE time >= "2023-12-04 12:19:00" AND
# MAGIC       time <= "2023-12-04 13:01:20";

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
# MAGIC
# MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
# MAGIC
# MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
# MAGIC
# MAGIC 4. Here you should see the entire query plan.
# MAGIC
# MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy.*your schema*.iot_data_partitioned (1)** and select the plus icon.
# MAGIC
# MAGIC #### Look at the following metrics in the Spark UI (results may vary):
# MAGIC | Metric    | Value    | Note    |
# MAGIC |-------------|-------------|-------------|
# MAGIC | cloud storage request count total (min, med, max)| 2500 (21, 37, 37)| Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data. <br></br>Monitoring this metric helps optimize performance, reduce costs, and identify potential inefficiencies in data access patterns. <br></br>The min, med and max represent the summary of requests made by tasks or executors. The distribution is fairly uniform across tasks or executors and there is not a large variance in the number of cloud storage requests made by each task.|
# MAGIC | cloud storage response size total (min, med, max)| 2.1 MiB (18.0 KiB, 31.8 KiB, 31.8 KiB)| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.<br></br> The min,med and max indicate most tasks transferring between 18.0 KiB and 31.8 KiB of data, showing a relatively consistent and uniform data transfer pattern across tasks.|
# MAGIC | **files pruned** | **0** |A total of 0 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being partitioned by **id** but queried by the **time** column. No files were pruned in this query.|
# MAGIC | **files read** | **2,500**| 2,500 files were read during the execution of the Spark job. This is because of the query was executed on the **time** column and the table is partitioned **id** column. In this query, all files were read into Spark then filtered for the necessary rows.|
# MAGIC
# MAGIC #### Summary
# MAGIC Because the data was partitioned by **id** but queried by the **time** column, Spark read all of the files to perform the required query and filter the data to return a single row.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Fixing the Problem
# MAGIC
# MAGIC Up to this point, we have been working with 2,500 rows of data that were partitioned in the table.
# MAGIC
# MAGIC We are now going to increase the volume dramatically by using 50,000,000 rows. If we had tried the code above with a dataset this large, it would have taken considerably longer to create all of the partitions (directories for each partition).
# MAGIC
# MAGIC As before, the following cell generates the data.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

df = (spark
      .range(0,50000000, 1, 32)
      .select(
          hash('id').alias('id'), # randomize our ids a bit
          rand().alias('value'),
          from_unixtime(lit(1701692381 + col('id'))).alias('time') 
      )
    )

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now we'll create a table named **iot_data** to capture the data, **this time without partitioning**. Doing it this way accomplishes the following:
# MAGIC - Takes less time to run, even on larger datasets, because we are not creating a high number of table partitions.
# MAGIC - Writes fewer files (32 files for 50,000,000 rows vs. 2,500 files for 2,500 rows in the partitioned table).
# MAGIC - Writes faster compared to disk partitioning because all files are in one directory instead of creating 2,500 directories.
# MAGIC - Queries for one **id** in about the same time as before.
# MAGIC - Filters by the **time** column much faster since it only has to query one directory.
# MAGIC

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS iot_data')

(df
 .write
 .option("overwriteSchema", "true")
 .mode('overwrite')
 .saveAsTable("iot_data")
)

display(spark.sql('SELECT count(*) FROM iot_data'))

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the history of the **iot_data** table. Confirm the following:
# MAGIC - In the **operationParameters** column, confirm the table is not partitioned.
# MAGIC - In the **operationMetrics** column, confirm the table contains 32 total files in the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Validate Optimization
# MAGIC The next two cells repeat the queries from earlier and will put this change to the test. The first cell should run almost as fast as before, and the second cell should run much faster.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Query 1 - Filter by the id column (non partitioned table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM iot_data 
# MAGIC WHERE id = 519220707

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how this query performed using the Spark UI. Compare the results against the same query we performed earlier against an over-partitioned table.
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
# MAGIC
# MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
# MAGIC
# MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
# MAGIC
# MAGIC 4. Here you should see the entire query plan.
# MAGIC
# MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy.*your schema*.iot_data (1)** and select the plus icon.
# MAGIC
# MAGIC #### Look at the following metrics in the Spark UI (results may vary):
# MAGIC | Metric    | Value    | Note    |
# MAGIC |-------------|-------------|-------------|
# MAGIC | cloud storage request count total (min, med, max)| 65 (8, 8, 9)|  Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data. <br></br>Monitoring this metric helps optimize performance, reduce costs, and identify potential inefficiencies in data access patterns.<br></br>The request count distribution is quite uniform across tasks/executors, as the min, med, and max values are very close to each other (8 and 9) indicating cloud storage access was consistent during execution.|
# MAGIC | cloud storage response size total (min, med, max)| 216.9 MiB (24.8 MiB, 24.8 MiB, 43.0 MiB)| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
# MAGIC | files pruned | 0 |A total of 0 files were skipped by Spark due to pruning based on the query's filters. This is because no optimized saving table techniques were used for the table.|
# MAGIC | files read | 32| 32 files were read during the execution of the Spark job. |
# MAGIC
# MAGIC #### Summary
# MAGIC In this example, we had 50,000,000 rows (more than the original 2,500 rows) but only 32 files and no partition in the table. While this table had many more rows, Spark only had 32 files and no partitions to query, avoiding the small file problem we encountered in the partitioned table, enabling the query to run fast.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Query 2 - Filter by the time column (non partitioned table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(value) 
# MAGIC FROM iot_data 
# MAGIC WHERE time >= "2023-12-04 12:19:00" AND 
# MAGIC       time <= "2023-12-04 13:01:20";

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how this query performed using the Spark UI. Compare the results against the same query we performed earlier against an over-partitioned table.
# MAGIC
# MAGIC 1. In the cell above, expand **Spark Jobs**.
# MAGIC
# MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
# MAGIC
# MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
# MAGIC
# MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
# MAGIC
# MAGIC 4. Here you should see the entire query plan.
# MAGIC
# MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy.*your schema*.iot_data (1)** and select the plus icon.
# MAGIC
# MAGIC #### Look at the following metrics in the Spark UI (results may vary):
# MAGIC
# MAGIC | Metric    | Value    | Note    |
# MAGIC |-------------|-------------|-------------|
# MAGIC | cloud storage request count| 3| Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data. |
# MAGIC | cloud storage response size| 	18.4 MiB| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
# MAGIC | files pruned | 31 | Spark determined  that 31 files did not contain any relevant data based on the WHERE condition filter for the **time** column. |
# MAGIC | files read | 1 | Spark only read 1 of the files from cloud storage. |
# MAGIC
# MAGIC #### Summary
# MAGIC In this example, we had 50,000,000 (more than the original 2,500 rows) but only 32 files in the table. While this table had many more rows, Spark only had 32 files to query, pruning almost all the files based on the **time** column, avoiding the small file problem we encountered in the partitioned table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo Summary
# MAGIC In the **iot_data** table, we did not partition the table when saving it. In this example, we allowed Spark to handle the saving process. Even though the dataset was much larger than the partitioned table from the first example, Spark optimized how the data was saved. It created 32 files for the Delta table, with each file containing a balanced number of rows, thus avoiding the "small file" problem that occurred with the partitioned table in the earlier example.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>