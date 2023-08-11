# Databricks notebook source
from logger.loggingCode import loggingCodeClass

l = loggingCodeClass()
logger = l.getlogger("my notebook imported logger")
logger.info("this is the info")

# COMMAND ----------

def readData(dbfsPath):
    
