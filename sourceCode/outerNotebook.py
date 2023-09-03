# Databricks notebook source
import time
import pandas as pd
from datetime import datetime
import  pyspark.sql.functions as F
from sourceCode.pipelines.logger.loggingCode import loggingCodeClass

l = loggingCodeClass()
logger = l.getlogger("outerNotebook")

# COMMAND ----------

def checkPathExists_def(path):
#In this function we just check if there are any files arrived yet or not. If yes then we proceed
    try:
        if len(dbutils.fs.ls(path)) == 0:
            logger.info(f"checkPathExists_def() : No files arrived as of {datetime.now()} in - {path}")
        
        elif len(dbutils.fs.ls(path)) > 0:
            logger.info(f"checkPathExists_def() : Files found : Calling checkInitialFiles_def()")
            checkInitialFiles_def(path)

    except Exception as e:
        logger.error(f"checkPathExists_def() : {e}")

# COMMAND ----------

def checkInitialFiles_def(path):
# In this function we check for the first time if there are any files arrived
    firstBatch = 0
    try:
        
        if spark.catalog.tableExists("watermark_tbl"):
            logger.info(f"checkInitialFiles_def() : watermark_tbl exists")
            # # ?? write function to process the files and pass the bacth to the function. Go to checkInitialFiles_def() to collect new files.
            logger.info(f"checkInitialFiles_def() : Calling checkNewFiles_def()")
            batch = checkNewFiles_def(path)
            # # two taks if new files found - first do the data cleaning processing, then update the watermark table with new entries
            if batch == 0:
                logger.info(f"checkInitialFiles_def() : No New Files Arrived")

            elif batch.count() > 0:
                
                logger.info(f"checkInitialFiles_def() : Calling processBatchCleaning_def() for current batch")
                if processBatchCleaning_def(batch, firstBatch):
                    logger.info(f"checkInitialFiles_def() : Updating watermark_tbl for current batch")
                    updateWatermark(batch)
           
            # return batch

        else:
            firstBatch = 1

            logger.info("checkInitialFiles_def() : Converting DataType of Column modificationTime for first batch")
            pandas_df = pd.DataFrame(dbutils.fs.ls("/FileStore/tables/hellofresh"))  # Convert dbutils files list in pandas dataframe
            pandas_df['modificationTime'] = pandas_df['modificationTime'].apply(lambda x : datetime.fromtimestamp(int(str(x).replace('000', ''))))
            # Used lambda to convert type of column modificationTime from string to datetime
            batch = spark.createDataFrame(pandas_df)

            logger.info("checkInitialFiles_def() : Calling processBatchCleaning_def() function and checking it it returns True")

            if processBatchCleaning_def(batch, firstBatch):
                
                logger.info("checkInitialFiles_def() : processBatchCleaning_def() succeeded : Now creating Table watermark_tbl ")
                batch.write.saveAsTable("watermark_tbl")
                logger.info("watermark_tbl created")

    except Exception as e:
        logger.error(f"checkInitialFiles_def() : {e}")

# COMMAND ----------

def checkNewFiles_def(path):
# In this we check if there is any new files based on the last modificationTime from watermark_tbl and return path of all new files
    try:

        dfFiles_p = pd.DataFrame(dbutils.fs.ls(path))
        dfFiles_p['modificationTime'] = dfFiles_p['modificationTime'].apply(lambda x : datetime.fromtimestamp(int(str(x).replace('000', ''))))
        dfFiles_s = spark.createDataFrame(dfFiles_p).createOrReplaceTempView("newFiles_tmp") # Creating a temp view of available files in DBFS

        logger.info(f"checkNewFiles_def() : Last watermark found - {spark.sql('select max(modificationTime) from watermark_tbl').collect()[0][0]}")
        
        dfFilesBatch = spark.sql(f"SELECT * FROM newFiles_tmp WHERE modificationTime > (select max(modificationTime) from watermark_tbl)")  #== >

        if dfFilesBatch.count() == 0:
            logger.info(f"checkNewFiles_def() : No modificationTime found greater than last modificationTime in watermark_tbl")
            return 0

        else:
            return dfFilesBatch
    
    except Exception as e:
        logger.error(f"checkNewFiles_def() : {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dev trial

# COMMAND ----------

def processBatchCleaning_def(batch, firstBatch):
# Whenever a new file/batch is arrived, this function will be called with the paths of the new files to process them further.

    batchPaths = set()
    logger.info(f"processBatchCleaning_def() : Starting the Cleaning of current batch")

    try:
        for i in batch.collect():
            batchPaths.add(i[0])
        
        logger.info(f"processBatchCleaning_def() : Applying Transformation on batch and writing in raw location")
        (spark.read
         .json(list(batchPaths))) \
         .select(F.regexp_replace("cookTime", "PT", "").alias("cookTime"), \
              F.to_date("datePublished", "yyyy-MM-dd").alias("datePublished"), \
              F.trim("description").alias("description"), \
              F.trim("image").alias("image"), \
              F.trim("ingredients").alias("ingredients"), \
              F.trim("name").alias("name"), \
              F.regexp_replace("prepTime", "PT", "").alias("prepTime"), \
              F.trim("recipeYield").alias("recipeYield"), \
              F.trim("url").alias("url") \
        ) \
         .select(F.regexp_replace(F.regexp_replace("cookTime", "M", ""), "H", "*60+").alias("cookTime"), \
              F.to_date("datePublished", "yyyy-MM-dd").alias("datePublished"), \
              F.trim("description").alias("description"), \
              F.trim("image").alias("image"), \
              F.trim("ingredients").alias("ingredients"), \
              F.trim("name").alias("name"), \
              F.regexp_replace(F.regexp_replace("prepTime", "M", ""), "H", "*60+").alias("prepTime"), \
              F.trim("recipeYield").alias("recipeYield"), \
              F.trim("url").alias("url") \
             ) \
         .write.mode("append") \
             .format('delta') \
             .option("checkpointLocation", "/FileStore/tables/checkpoint/dataCleaning_n") \
             .save("/FileStore/raw")

        logger.info(f"processBatchCleaning_def() : Transformation Succesfull : Files Written in Raw Layer for this batch")

        return True

    except Exception as e:

        logger.error(f"processBatchCleaning_def() : {e} ")

# COMMAND ----------

# batch = checkInitialFiles_def("/FileStore/tables/hellofresh")
checkPathExists_def("/FileStore/tables/hellofresh")
# if batch == 0:
#     updateWatermark(batch)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/tables/hellofresh/", True)
# dbutils.fs.rm("/FileStore/raw", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM watermark_tbl
# MAGIC -- drop table watermark_tbl

# COMMAND ----------

def updateWatermark(batch):
#This function basically Updates watermark table after each cleaning batch process succeeds.
        
    logger.info(f"processBatchCleaning_def() : raw layer updated : Creating temp view for batch of files")
    insertBatch = batch.createOrReplaceTempView("newInserts_v")

    logger.info(f"processBatchCleaning_def() : Running Insert Query to update watermark_tbl")
    spark.sql(f"INSERT INTO watermark_tbl ( SELECT * FROM newInserts_v)")

    logger.info(f"processBatchCleaning_def() : Insert Complete....")

# COMMAND ----------

dfFiles_p = pd.DataFrame(dbutils.fs.ls("dbfs:/FileStore/tables/hellofresh/"))
dfFiles_p['modificationTime'] = dfFiles_p['modificationTime'].apply(lambda x : datetime.fromtimestamp(int(str(x).replace('000', ''))))
dfFiles_s = spark.createDataFrame(dfFiles_p)
display(dfFiles_s)
