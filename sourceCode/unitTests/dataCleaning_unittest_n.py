# Databricks notebook source
# MAGIC %run ../pipelines/dataCleaning_n

# COMMAND ----------

def checkNewFiles_def_test():
#In this we test the checkNewFiles_def(path) method by passing a file path which is the last file

    assert checkNewFiles_def("/FileStore/tables/hellofresh/recipes_002.json") == 0, f"Make sure the file path passed in function have timestamp not bigger than already existing files"
    assert spark.sql("SELECT * FROM newFiles_tmp").count() > 0, f"The temp view newFiles_tmp does not exists"

# COMMAND ----------

dbutils.fs.rm("/FileStore/check/recipes_000.json", True)

# COMMAND ----------

def checkPathExists_def_test():
# In this function we just check by passing an empty path if the function returns 0.
    assert checkPathExists_def("/FileStore/check/") == 0, f"Error with checkPathExists_def()"

# COMMAND ----------

checkNewFiles_def_test()
checkPathExists_def_test()
