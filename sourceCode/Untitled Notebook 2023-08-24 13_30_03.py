# Databricks notebook source
partition_size = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")
print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")

# COMMAND ----------

# Check the default parallelism available
print(f"Parallelism : {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# File size that we are going to import
import os
file_size = os.path.getsize('/dbfs/FileStore/tables/hellofresh/recipes_002.json')
print(f"""Data File Size: 
            {file_size} in bytes 
            {int(file_size) / 1024 / 1024} in MB
            {int(file_size) / 1024 / 1024 / 1024} in GB""")

# COMMAND ----------

# Lets create a simple Python decorator - {get_time} to get the execution timings
# If you dont know about Python decorators - check out : https://www.geeksforgeeks.org/decorators-in-python/
import time

def get_time(func):
    def inner_get_time() -> str:
        start_time = time.time()
        func()
        end_time = time.time()
        print("-"*80)
        return (f"Execution time: {(end_time - start_time)*1000} ms")
    print(inner_get_time())
    print("-"*80)

# COMMAND ----------

# Lets read the file and write in noop format for Performance Benchmarking
@get_time
def x():
    df = spark.read.format("csv").option("header", True).load("/FileStore/tables/hellofresh/recipes_002.json")
    print(f"Number of Partition -> {df.rdd.getNumPartitions()}")
    df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# Change the default partition size to 3 times to decrease the number of partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 3 * 1024 * 1024)+"b")

# Verify the partition size
partition_size = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")
print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")
