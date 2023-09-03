# Databricks notebook source
import pandas as pd

# COMMAND ----------

df = spark.read \
    .load("/FileStore/raw/")

display(df.dtypes)
# print(df.dtypes)

# COMMAND ----------

pan = pd.DataFrame(df.dtypes)
print(pan[0])

# COMMAND ----------

cols = {}

# COMMAND ----------

cookTime, datePublished, description, image, ingredients, name, prepTime, recipeYield, url
