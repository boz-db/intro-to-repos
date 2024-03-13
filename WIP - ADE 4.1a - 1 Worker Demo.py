# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md --i18n-d6d37384-055d-4f2e-85c6-7e10efe30a02
# MAGIC
# MAGIC # 1 Worker Demo
# MAGIC
# MAGIC This notebook, along with ADE 4.1b - 2 Workers Demo, will demonstrate horizontal scaling. 
# MAGIC
# MAGIC ### Horizontal Scaling
# MAGIC
# MAGIC In the cloud you are renting compute, and with Databricks it's very easy to add more compute to more quickly finish workloads.  With these two notebooks we'll see the impact doubling the cluster size has on our workload.  
# MAGIC
# MAGIC ### Cost Impact
# MAGIC
# MAGIC It's important to note that if this were a pipeline, the cost of a larger cluster may not be any more than the cost of a smaller cluster since the larger cluster finishes and shuts down sooner.  So you pay more for the cluster while it's up, but you pay for a shorter time.
# MAGIC
# MAGIC ### Cluster Config (Photon - single node)
# MAGIC Run this notebook with the same cluster configuration as used by most notebooks (described in the ADE 4.0 - Module Introduction).  For Azure use the `Standard_L4s` instance type.  (Side note: Technically this is a 0 worker cluster, but it performs similarly to a 1 worker cluster.  We're using this configuration because it's inexpensive and reused across most of the notebooks)
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://petecurriculumstorage.blob.core.windows.net/images/photon_config.png" alt="Photon Cluster Config" width="600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.1a

# COMMAND ----------

# Turn off caching for more consistent experiments
spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing
# MAGIC
# MAGIC Let's see if horizontal scaling helps with pipeline-style data processing.  We have data about all of the flights in the USA, and we'll add a column to it and save that to another table as an exercise.  Let's first take a look at the input data:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbacademy.flights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a Column
# MAGIC Let's add the column 'VeryLate', which is True if the flight arrives more than 30 minutes late.  Let's see what that looks like:

# COMMAND ----------

from pyspark.sql.functions import *

flights_verylate_df = (spark
                        .table('dbacademy.flights')
                        .filter("year = 2004")
                        .withColumn('VeryLate', col('ArrDelay') > 30)
                      )

flights_verylate_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the data
# MAGIC
# MAGIC Now let's save our table with the new column as *flights_verylate*.  This is when the processing actually happens.  Let's see how long this takes with only 1 worker at our disposal:

# COMMAND ----------

(flights_verylate_df
    .filter("year = 2004")
    .write
    .mode('overwrite')
    .saveAsTable('flights_verylate')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join
# MAGIC
# MAGIC We'll want to see how horizontal scaling impacts join operations as well.  The next few cells will create a new dataset for this experiment:

# COMMAND ----------

from pyspark.sql.functions import *

transactions_df = (spark
                        .range(0, 300000000, 1, numPartitions=32)
                        .select(
                            'id',
                            round(rand() * 10000, 2).alias('amount'),
                            (col('id') % 10).alias('country_id')
                        )
                    )

transactions_df.display()

# COMMAND ----------

transactions_df.write.mode('overwrite').saveAsTable('transactions')

# COMMAND ----------

countries = [(0, "Italy"),
             (1, "Canada"),
             (2, "Mexico"),
             (3, "China"),
             (4, "Germany"),
             (5, "UK"),
             (6, "Japan"),
             (7, "Korea"),
             (8, "Australia"),
             (9, "France"),
             (10, "Spain"),
             (11, "USA")
            ]

columns = ["id", "name"]
countries_df = spark.createDataFrame(data = countries, schema = columns)

countries_df.display()

# COMMAND ----------

countries_df.write.saveAsTable("countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform the Join
# MAGIC
# MAGIC Now that we have the data, let's join our transactions table to our countries table.  This next cell will perform the join and save it to the *transact_countries* table.  Make note of how long this takes.  We will want to compare it to our 2 workers notebook.

# COMMAND ----------

# TODO

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

transactions_df = spark.table('transactions')
countries_df = spark.table('countries')

joined_df = (transactions_df
                            .join(
                                countries_df,
                                on=transactions_df.country_id == countries_df.id
                                )
                            .select(transactions_df.id, 'amount', 'name')
            )

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation
# MAGIC
# MAGIC Aggregation is another area where we'll want to see if horizontal scaling helps.  Let's calculate the average flight delay per year as an experiment.  This next cell is another good one to compare to the 2 workers notebook:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC     year,
# MAGIC     avg(ArrDelay) as avg_delay
# MAGIC from dbacademy.flights
# MAGIC group by year
# MAGIC sort by avg_delay desc

# COMMAND ----------


