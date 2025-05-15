-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab: Data Skipping and Liquid Clustering
-- MAGIC
-- MAGIC In this demo, we are going to work with Liquid Clustering, a Delta Lake optimization feature that replaces table partitioning and ZORDER to simplify data layout decisions and optimize query performance. It provides flexibility to redefine clustering keys without rewriting data. Refer to the [documentation](https://docs.databricks.com/en/delta/clustering.html) for more information.
-- MAGIC
-- MAGIC #### Learning Objectives
-- MAGIC **By the end of this lab, you will be able to:**
-- MAGIC
-- MAGIC * Disable Spark caching to observe the effects of Liquid Clustering.
-- MAGIC * Count records and explore data in the flights tables.
-- MAGIC * Execute queries on an unclustered table and analyze their performance using Spark UI.
-- MAGIC * Execute and compare queries on tables clustered by different columns (**id** and **id** + **FlightNum**).
-- MAGIC * Inspect query performance using the Spark UI to understand the benefits of Liquid Clustering.
-- MAGIC
-- MAGIC #### Prerequisites
-- MAGIC In order to follow along with this lab, you will need:
-- MAGIC
-- MAGIC * Basic knowledge of running SQL queries in Databricks is required.
-- MAGIC * Familiarity with Delta Lake and its optimization features is recommended.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default. If you use Serverless, errors will be returned when setting compute runtime properties.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC   - In the drop-down, select **More**.
-- MAGIC
-- MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 1. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique **labuser** catalog, and the default schema to **default**. All tables will be read from and written to this location.
-- MAGIC <br></br>
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-2L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Set the default catalog to **dbacademy_flightdata** and the schema to **v01**. We will use the read-only tables in this location for the demonstration.
-- MAGIC
-- MAGIC **NOTE:** These tables are shared through the Databricks Marketplace, provided by **Databricks**. The name of the share is **Airline Performance Data**.

-- COMMAND ----------

USE CATALOG dbacademy_flightdata;
USE SCHEMA v01;

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Disable Caching
-- MAGIC
-- MAGIC Run the following cell to set a Spark configuration variable that disables disk caching.
-- MAGIC
-- MAGIC Turning disk caching off prevents Databricks from storing cloud storage files after the first query. This makes the effect of the optimizations more apparent by ensuring that files are always pulled from cloud storage for each query.
-- MAGIC
-- MAGIC For more information, see [Optimize performance with caching on Databricks](https://docs.databricks.com/en/optimizations/disk-cache.html#optimize-performance-with-caching-on-databricks).
-- MAGIC
-- MAGIC **NOTE:** This will not work in Serverless. Please use classic compute to turn off caching. If you're using Serverless, an error will be returned.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the spark configuration variable "io.cache" as "False"
-- MAGIC spark.conf.set('spark.databricks.io.cache.enabled', False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Inspecting the tables with and without clustering, using Spark UI to identify that data is skipped at the source.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Explore the Data
-- MAGIC In this lab, we will be using airline flight data that has been saved in three different ways:
-- MAGIC - **flights**: OPTIMIZED with a ZORDER on **FlightNum**.
-- MAGIC - **flights_cluster_id**: Liquid clustered using the **id** column.
-- MAGIC - **flights_cluster_id_flightnum**: Liquid clustered by two columns (**id** and **FlightNum**).
-- MAGIC
-- MAGIC Each table contains the exact same data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Preview the **flights** table. Notice that it contains the following columns: **id, year, FlightNum, ArrDelay, UniqueCarrier, TailNum**.
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM flights
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Count the number of rows in each of the flight tables (**flights, flights_cluster_id, flights_cluster_id_flightnum**) to confirm that the number of rows in each table is *1,235,347,780*.
-- MAGIC
-- MAGIC     Each table contains the same data but is stored differently.
-- MAGIC

-- COMMAND ----------

SELECT 'flights', count(*) AS TotalRows
FROM flights
UNION ALL
SELECT 'flights_cluster_id', count(*) AS TotalRows
FROM flights_cluster_id
UNION ALL
SELECT 'flights_cluster_id_flightnum', count(*) AS TotalRows
FROM flights_cluster_id_flightnum;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Query Performance with ZORDER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute the cell below to view the history of the **flights** table. In the output, observe the following:
-- MAGIC - In the **operation** column, find the version (row) of the Delta table that has been *OPTIMIZED*.
-- MAGIC
-- MAGIC - In the **operationParameters** column, notice that the **flights** table has been optimized with a ZORDER on the **FlightNum** column. Z-ordering is a technique used to colocate related information in the same set of files.
-- MAGIC
-- MAGIC - In the **operationMetrics** column, observe that the OPTIMIZED statement removed (*numRemovedFiles*) *160* files and added (*numAddedFiles*) *31* files to optimize the storage of the table.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY flights;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.1 Unique Carrier Column Query
-- MAGIC 1. Run the following query to analyze the average arrival delay in flights by querying the **UniqueCarrier** column for the *TW* airline. Take note of the time it took for the query to execute.
-- MAGIC
-- MAGIC **NOTE:** The **flights** table contains a ZORDER on **FlightNum**.

-- COMMAND ----------

-- Perform the SELECT operation with the AVG operation on the 'ArrDelay' column and the 'UniqueCarrier' set to 'TW'.

SELECT AVG(ArrDelay)
FROM flights
WHERE UniqueCarrier = 'TW';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 149 (3, 3, 18)|  Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.<br></br>Monitoring this metric helps optimize performance, reduce costs, and identify potential inefficiencies in data access patterns.|
-- MAGIC | cloud storage response size total (min, med, max)|1068.4 MiB (336.6 KiB, 45.7 MiB, 50.7 MiB)| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.<br></br> The data transferred ranged from small to large requests, with an average response size around 45.7 MiB.|
-- MAGIC | files pruned | 0 |A total of 0 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being z-ordered by **FlightNum** but queried by the **UniqueCarrier** column.|
-- MAGIC | files read | 31| All 31 files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was Z-ordered by **FlightNum** but queried by the **UniqueCarrier** column. Spark needs to read all of the files to filter the table and return a single row. On average, this query will take about ~10 seconds to complete.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.2 FlightNum Column (ZORDER column) Query
-- MAGIC 1. Run the following query to analyze the average arrival delay by flight number (**FlightNum**) *1809*. Take note of the time it took for the query to execute.
-- MAGIC

-- COMMAND ----------

-- Perform the SELECT operation with the AVG operation on the 'ArrDelay' column and the 'FlightNum' set to '1890'.
SELECT AVG(ArrDelay) 
FROM flights
WHERE FlightNum = 1890

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 6 (3, 3, 3)|  Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | cloud storage response size total (min, med, max)|53.5 MiB (26.3 MiB, 27.2 MiB, 27.2 MiB)| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.<br></br> The min, med and max suggest that the cloud storage requests were relatively consistent in size.|
-- MAGIC | files pruned | 30 |A total of 30 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being z-ordered by **FlightNum** and queried by the **FlightNum** column.|
-- MAGIC | files read | 1| Only 1 file was read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was Z-ordered by **FlightNum** and queried by the **FlightNum** column. In this scenario, the files were organized by **FlightNum** and queried by **FlightNum**, so it was optimized for the query to efficiently read the necessary files. On average, this query will take about ~2 seconds to complete.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.3 id Column Query
-- MAGIC 1. Run the following query to analyze the records in the flight table with the **id** *1125281431554*. Take note of the time it took for the query to execute.
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM flights
WHERE id = 1125281431554;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 267 (1, 4, 14)| Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | **cloud storage response size total (min, med, max)**|**4.7 GiB (256.0 KiB, 72.5 MiB, 101.1 MiB)**| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.<br></br> The total data transferred is extremely large at around 4.7 GiB and the individual request sizes vary greatly.|
-- MAGIC | files pruned | 0 |A total of 0 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being z-ordered by **FlightNum** and queried by the high cardinality **id** column.|
-- MAGIC | files read | 31| All files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was Z-ordered by **FlightNum** and queried by the **id** column. The **id** column is a high-cardinality column, which causes the query to:
-- MAGIC - Have a very large cloud storage response size.
-- MAGIC - Read every file.
-- MAGIC - Run for around ~28 seconds.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Explore Liquid Clustering on Tables
-- MAGIC Delta Lake liquid clustering replaces table partitioning and ZORDER to simplify data layout decisions and optimize query performance. It provides flexibility to redefine clustering keys without rewriting existing data, allowing data layout to evolve alongside analytic needs over time.  
-- MAGIC
-- MAGIC Databricks recommends liquid clustering for all new Delta tables. Scenarios benefiting from clustering include:
-- MAGIC
-- MAGIC * Tables often filtered by high cardinality columns.
-- MAGIC * Tables with a significant skew in data distribution.
-- MAGIC * Tables that grow quickly and require maintenance and tuning effort.
-- MAGIC * Tables with concurrent write requirements.
-- MAGIC * Tables with access patterns that change over time.
-- MAGIC * Tables where a typical partition key could leave the table with too many or too few partitions.
-- MAGIC
-- MAGIC For more information on how to enable the liquid clustering, refer to the [documentation](https://docs.databricks.com/en/delta/clustering.html#enable-liquid-clustering)
-- MAGIC
-- MAGIC **NOTE:** These queries on the ZORDERED table are already quite fast without using clustering, considering we are using a small cluster, and the tables are not extremely large. But there is room for improvement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C3.1 Query Performance with Liquid Clustering (id column)
-- MAGIC Run the following three queries. These queries pull data from the **flights_cluster_id** table. This table is exactly the same as the one we used in the queries above, except that we have enabled liquid clustering by adding `CLUSTER BY (id)` when the table was created.  
-- MAGIC   
-- MAGIC Note the following:
-- MAGIC - When we query by the clustered column (**id**), we see an improvement in query performance
-- MAGIC - We don't see a degradation in performance on queries against unclustered columns  
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute the cell below to view the history of the **flights_cluster_id** table. In the output, observe the following:
-- MAGIC - In the **operationParameters** column, notice that the **flights_cluster_id** table has been optimized with a *clusterBy* on the **ID** column.
-- MAGIC
-- MAGIC - In the **operationMetrics** column, notice that the clustered table was created with *128* files.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY flights_cluster_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View detailed metadata about the **flights_cluster_id** table. Notice the following:
-- MAGIC - The table contains the same 6 columns.
-- MAGIC - Under *#Clustering Information*, we see that the table is clustered by **id**.
-- MAGIC - At the bottom of the results, the **Table Properties** contain a variety of properties for liquid clustering.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED flights_cluster_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### C3.1.1 Unique Carrier Column Query
-- MAGIC 1. Run the following query to analyze the average arrival delay in flights for the *TW* airline. Take note of the time it took for the query to execute.
-- MAGIC
-- MAGIC     **NOTE:** The **flights_cluster_id** table is clustered by **id**.
-- MAGIC

-- COMMAND ----------

-- Perform the SELECT operation on the 'flights_cluster_id' table with the AVG operation on the 'ArrDelay' column and the 'UniqueCarrier' set to 'TW'.
SELECT AVG(ArrDelay) 
FROM flights_cluster_id
WHERE UniqueCarrier = 'TW';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights_cluster_id (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 316 (4, 8, 12)|  Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | cloud storage response size total (min, med, max)|1047.8 MiB (601.0 KiB, 27.5 MiB, 47.2 MiB) | Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
-- MAGIC | files pruned | 0 |A total of 0 files were skipped by Spark due to pruning based on the query's filters.|
-- MAGIC | files read | 128 | All files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was clustered by **id** and queried by the **UniqueCarrier** column, taking about ~10 seconds to execute. The execution time and cloud storage response size are very similar to the query on the optimized **flights** table with a ZORDER on **FlightNum**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### C3.1.2 FlightNum Column Query
-- MAGIC 1. Run the following query to analyze the average arrival delay by flight number (**FlightNum**) *1809*. Take note of the time it took for the query to execute.
-- MAGIC

-- COMMAND ----------

-- Perform the SELECT operation on the 'flights_cluster_id' table with the AVG operation on the 'ArrDelay' column and the 'FlightNum' set to '1890'.
SELECT AVG(ArrDelay) 
FROM flights_cluster_id
WHERE FlightNum = 1890;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights_cluster_id (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 	275 (4, 12, 15)|Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | cloud storage response size total (min, med, max)|1871.3 MiB (22.8 MiB, 85.3 MiB, 94.2 MiB) | Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
-- MAGIC | files pruned | 0 |A total of 0 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being clustered by **id** and queried by the **FlightNum** column.|
-- MAGIC | files read | 128 | All files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was clustered by **id** and queried by the **FlightNum** column, taking about ~10 seconds to execute. This query took a bit longer to execute, and the cloud storage response size was much larger than the query on the optimized **flights** table with a ZORDER on **FlightNum**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### C3.1.3 id Column Query
-- MAGIC 1. Run the following query to analyze the records in the flight table with the **id** `1125281431554`. Take note of the time it took for the query to execute. Remember, the query on the **id** column in the **flights** table took about ~28 seconds to execute.
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM flights_cluster_id
WHERE id = 1125281431554

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights_cluster_id (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total | 4| Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | cloud storage response size total |54.5 MiB| Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
-- MAGIC | files pruned | 127 |A total of 127 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being clustered by **id** and queried by the **id** column.|
-- MAGIC | files read | 1| Only 1 file was read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was clustered by **id** and queried by the **id** column. This enables Spark to optimally read the data and execute in around ~2 seconds, compared to ~28 seconds for the **flights** table, which contained a ZORDER on **FlightNum** and was optimized. The cloud storage response size was also much smaller than 4.7 GiB.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C3.2 Query Performance with Liquid Clustering (id and flight_num columns)
-- MAGIC Run the following three queries. These queries pull data from the **flights_cluster_id_flightnum** table. This table is clustered by both the **id** and **flight_num** columns.  
-- MAGIC
-- MAGIC Note the following:
-- MAGIC - We still don't have any degradation on unclustered columns. Had we used `PARTITION BY` to partition by **FlightNum** and **id**, we would see massive slowdown for any queries not on those columns, and writes would be prohibitively slow for this volume of data
-- MAGIC - Now queries on **FlightNum** are improved
-- MAGIC - Queries are a little slower on **id** now, however and we can look at the DAG to see why.
-- MAGIC
-- MAGIC Note that, because  we had to read more files to satisfy this request. There is a (small) cost to clustering on more columns, so choose wisely.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Execute the cell below to view the history of the **flights_cluster_id_flightnum** table. In the output, observe the following:
-- MAGIC - In the **operationParameters** column, notice that the **flights_cluster_id_flightnum** table has been optimized with a *clusterBy* on the **id** and **FlightNum** columns.
-- MAGIC - In the **operationMetrics** column, notice that the clustered table was created with *144* files.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY flights_cluster_id_flightnum

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View detailed metadata about the **flights_cluster_id_flightnum** table. Notice the following:
-- MAGIC - Under *#Clustering Information*, we see that the table is clustered by **id** and **FlightNum**.
-- MAGIC - At the bottom of the results, the **Table Properties** contain a variety of properties for liquid clustering.
-- MAGIC

-- COMMAND ----------

DESCRIBE TABLE EXTENDED flights_cluster_id_flightnum;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### C3.2.1 Unique Carrier Column Query
-- MAGIC 1. Run the following query to analyze the average arrival delay in flights for the *TW* airline. Take note of the time it took for the query to execute.
-- MAGIC

-- COMMAND ----------

-- Perform the SELECT operation on the 'flights_cluster_id' table with the AVG operation on the 'ArrDelay' column and the 'UniqueCarrier' set to 'TW'.
SELECT AVG(ArrDelay) 
FROM flights_cluster_id_flightnum
WHERE UniqueCarrier = 'TW';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights_cluster_id_flightnum (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 374 (3, 10, 14)| Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | cloud storage response size total (min, med, max)|997.6 MiB (988.3 KiB, 27.0 MiB, 43.7 MiB) | Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
-- MAGIC | files pruned | 0 |A total of 0 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being clustered by **id** and **FlightNum** and queried by the **UniqueCarrier** column.|
-- MAGIC | files read | 144 | All files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was clustered by **id** and **FlightNum**, and queried by the **UniqueCarrier** column. This query should run in about ~10 seconds. Similar to the other two queries performed on the **UniqueCarrier** column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### C3.2.2 FlightNum Column Query
-- MAGIC 1. Run the following query to analyze the average arrival delay by flight number (**FlightNum**) *1809*. Take note of the time it took for the query to execute. Remember, the **flights_cluster_id_flightnum** table is clustered by **id** and **FlightNum**.
-- MAGIC

-- COMMAND ----------

-- Perform the SELECT operation on the 'flights_cluster_id' table with the AVG operation on the 'ArrDelay' column and the 'FlightNum' set to '1890'.
SELECT AVG(ArrDelay) 
FROM flights_cluster_id_flightnum
WHERE FlightNum = 1890;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights_cluster_id_flightnum (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 32 (4, 6, 6)|Refers to the number of requests made to the cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. This could involve multiple operations like reading metadata, accessing directories, or fetching the actual data.|
-- MAGIC | cloud storage response size total (min, med, max)|146.3 MiB (21.9 MiB, 24.9 MiB, 27.6 MiB) | Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
-- MAGIC | files pruned | 132 |A total of 132 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being clustered by **id** and **FlightNum**, and queried by the **FlightNum** column.|
-- MAGIC | files read | 12| Only 12 files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Summary
-- MAGIC This table was clustered by **FlightNum** and **id** and queried by the **FlightNum** column. This enables Spark to optimize how it reads the data. This query should execute in around ~2 seconds, similar to the **flights** table with a ZORDER on **FlightNum**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### C3.2.3 id Column Query
-- MAGIC 1. Run the following query to analyze the records in the flight table with the **id** *1125281431554*. Take note of the time it took for the query to execute. Remember, the **flights_cluster_id_flightnum** table is clustered by **id** and **FlightNum**.
-- MAGIC
-- MAGIC     Notice how quickly this query executes compared to the **flights** table.
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM flights_cluster_id_flightnum
WHERE id = 1125281431554

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time. To view how the query performed complete the following:
-- MAGIC
-- MAGIC 1. In the cell above, expand **Spark Jobs**.
-- MAGIC
-- MAGIC 2. Right click on **View** and select *Open in a New Tab*. 
-- MAGIC
-- MAGIC     **NOTE:** In the Vocareum lab environment if you click **View** without opening it in a new tab the pop up window will display an error.
-- MAGIC
-- MAGIC 3. In the new window, find the **Associated SQL Query** header at the top and select the number.
-- MAGIC
-- MAGIC 4. Here you should see the entire query plan.
-- MAGIC
-- MAGIC 5. In the query plan, scroll down to the bottom and find **PhotonScan parquet dbacademy_flightdata.v01.flights_cluster_id_flightnum (1)** and select the plus icon.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC #### Look at the following metrics in the Spark UI (results may vary slightly):
-- MAGIC | Metric    | Value    | Note    |
-- MAGIC |-------------|-------------|-------------|
-- MAGIC | cloud storage request count total (min, med, max)| 29 (2, 6, 6)|  Tracks the number of read and write requests made to cloud storage systems like S3, Azure Blob, or Google Cloud Storage during job execution. Monitoring this metric helps optimize performance, reduce costs, and identify potential inefficiencies in data access patterns.|
-- MAGIC | cloud storage response size total (min, med, max)|370.9 MiB (27.3 MiB, 68.5 MiB, 71.5 MiB) | Indicates the total amount of data transferred from cloud storage to Spark during the execution of a job. It helps track the volume of data read or written to cloud storage, providing insights into I/O performance and potential bottlenecks related to data transfer.|
-- MAGIC | files pruned | 134 |A total of 134 files were skipped by Spark due to pruning based on the query's filters. This is due to the table being clustered by **FlightNum** and **id**, and queried by the  **id** column.|
-- MAGIC | files read | 10 | All files were read during the execution of the Spark job.|
-- MAGIC
-- MAGIC
-- MAGIC #### Liquid Clustering Summary
-- MAGIC This table was clustered by **FlightNum** and **id** and queried by the **id** column. This enables Spark to optimize how it reads the data. This query should execute in around 2 seconds, much faster than the **flights** table with a ZORDER on **FlightNum** using the same query (~28 seconds).
-- MAGIC
-- MAGIC <br></br>
-- MAGIC ## NEWS! Automatic Liquid Clustering (Public Preview as of March 2025)!
-- MAGIC
-- MAGIC - [Announcing Automatic Liquid Clustering: Optimized data layout for up to 10x faster queries](https://www.databricks.com/blog/announcing-automatic-liquid-clustering) (Blog)
-- MAGIC
-- MAGIC - [Automatic liquid clustering](https://docs.databricks.com/aws/en/delta/clustering#automatic-liquid-clustering) documentation
-- MAGIC
-- MAGIC ![LQ Auto](./Includes/images/Liquid-Clusters-OG.png)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D. Lab Queries Summary (estimated average execution times below, execution times will vary)
-- MAGIC Please review the queries for each table below and compare some of the query statistics.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### D1. Query by UniqueCarrier
-- MAGIC **NOTES:** No storage optimization was set on the **UniqueCarrier** column, so all files were read for each table during the query. Each query had similar cloud storage response sizes and execution times.
-- MAGIC
-- MAGIC |Table| Query | Total Files | Files Read | Files Pruned | Query Duration (will vary) | cloud storage request count total | cloud storage response size total |
-- MAGIC |----------|----------|----------|----------|----------|----------|----------|----------|
-- MAGIC |**ZORDER by FlightNum**| `WHERE UniqueCarrier = 'TW'`    | 31   |31   |0   | ~12 s | 149  | 1068 MiB |
-- MAGIC |**CLUSTER BY id**| `WHERE UniqueCarrier = 'TW'`   | 128  | 128   | 0  | ~10 s | 316 | 1047.8 MiB |
-- MAGIC |**CLUSTER BY (id, FlightNum)**| `WHERE UniqueCarrier = 'TW'`  | 144  | 144   | 0 | ~10 s | 374  | 997.6 MiB |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### D2. Query by FlightNum
-- MAGIC **NOTES:** Notice that when the tables included **FlightNum** in a storage optimization technique, many of the files were pruned, increasing efficiency and speeding up execution times.
-- MAGIC
-- MAGIC The table that was only clustered by **id** ran slower, and the cloud storage response size was much larger.
-- MAGIC
-- MAGIC
-- MAGIC |Table| Query | Total Files | Files Read | Files Pruned | Query Duration (will vary) | cloud storage request count total | cloud storage response size total |
-- MAGIC |----------|----------|----------|----------|----------|----------|----------|----------|
-- MAGIC |**ZORDER by FlightNum**| `WHERE FlightNum = 1890`    | 31   |1   |30   | ~2 s | 6 |53.MiB |
-- MAGIC |**CLUSTER BY id**| `WHERE FlightNum = 1890`   | 128  | 128   | 0  | ~10 s	 |275  | **1871.3 MiB** |
-- MAGIC |**CLUSTER BY (id, FlightNum)**| `WHERE FlightNum = 1890`  | 144  | 12   | 132 | ~2 s| 32 | 146.3 MiB |
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### D3. Query by id
-- MAGIC **NOTES:** Notice that when the tables included **id** in a storage optimization technique (both liquid clustered tables), many of the files were pruned, increasing efficiency, even in the table that is clustered by both **id** and **FlightNum**. 
-- MAGIC
-- MAGIC You can also see that the cloud storage response size of each of the clustered tables was much smaller than the Z-ordered table by **FlightNum**.
-- MAGIC
-- MAGIC Also, notice that the Z-ordered table took an extremely long time to complete, and the cloud storage response size was much larger when querying the **id** column in the **flights** table.
-- MAGIC
-- MAGIC
-- MAGIC |Table| Query | Total Files | Files Read | Files Pruned | Query Duration (will vary) | cloud storage request count total | cloud storage response size total |
-- MAGIC |----------|----------|----------|----------|----------|----------|----------|----------|
-- MAGIC |**ZORDER by FlightNum**| `WHERE id = 1125281431554`     | 31   |31   |31   | **~28 s** |**267**  | **4.7 GiB** |
-- MAGIC |**CLUSTER BY id**| `WHERE id = 1125281431554`    | 128  | 1   | 127  | ~2 s | 4 |54.5 MiB |
-- MAGIC |**CLUSTER BY (id, FlightNum)**| `WHERE id = 1125281431554`   | 144  | 10   | 134 | ~2 s | 29 | 370.9 MiB |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>

-- COMMAND ----------

