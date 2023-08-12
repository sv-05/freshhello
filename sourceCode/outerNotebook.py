# Databricks notebook source
import time
import pandas as pd
from datetime import datetime
from sourceCode.pipelines.logger.loggingCode import loggingCodeClass

l = loggingCodeClass()
logger = l.getlogger("outerNotebook")
logger.info("this is the info")

# COMMAND ----------

# def checkIncomingFiles_df():
# def checkWatermarkTableExists()
def checkInitialFiles_def(path):
# In this function we check for the first time if there are any files arrived
    try:
        if len(dbutils.fs.ls(path)) == 0:
            logger.info(f"checkInitialFiles_def() : No files arrived as of {datetime.now()} in - {path}")

        elif len(dbutils.fs.ls(path)) > 0:
            if spark.catalog.tableExists("watermark_tbl"):
                logger.info("watermark_tbl exists")
                pass
            else:
                logger.info("Creating Table watermark_tbl")
                pandas_df = pd.DataFrame(dbutils.fs.ls("/FileStore/tables/hellofresh"))  # Convert dbutils files list in pandas dataframe
                pandas_df['modificationTime'] = pandas_df['modificationTime'].apply(lambda x : datetime.fromtimestamp(int(str(x).replace('000', ''))))
                # Used lambda to convert type of column modificationTime from string to datetime
                spark.createDataFrame(pandas_df).write.saveAsTable("watermark_tbl")
                logger.info("watermark_tbl created")

    except Exception as e:
        logger.error(f"checkInitialFiles_def() : {e}")

# COMMAND ----------

checkInitialFiles_def("/FileStore/tables/hellofresh")

# COMMAND ----------

def checkNewFiles_def(path):
# In this we check if there is any new files based on the last modificationTime from watermark_tbl and return path of all new files
    try:
        newBatch = set()
        dfFiles_p = pd.DataFrame(dbutils.fs.ls(path))
        dfFiles_p['modificationTime'] = dfFiles_p['modificationTime'].apply(lambda x : datetime.fromtimestamp(int(str(x).replace('000', ''))))

        for i in dfFiles_p.index:
            logger.info(f"checkNewFiles_def() : Creating batch of new files to process")
            if dfFiles_p['modificationTime'][i] > (spark.sql("select max(modificationTime) from watermark_tbl").collect())[0][0]:
                newBatch.add(dfFiles_p['path'][i])           # Collect all new arrived file paths
            else:
                logger.info(f"checkNewFiles_def() : No new files arrived - {datetime.now()}")
                
        return newBatch
    
    except Exception as e:
        logger.error(f"checkNewFiles_def() : {e}")

# COMMAND ----------

path = "/FileStore/tables/hellofresh"
checkNewFiles_def(path)
