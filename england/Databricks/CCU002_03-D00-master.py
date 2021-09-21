# Databricks notebook source
# MAGIC %md  # CCU002_03-D00-master
# MAGIC 
# MAGIC **Description** This notebook runs all notebooks in the correct order.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

# COMMAND ----------

# Step 1: preserve data used for present project

# dbutils.notebook.run("CCU002_03-D01-project_table_freeze", 3600)

# COMMAND ----------

# Step 2: preserve codelists used for present project

# dbutils.notebook.run("CCU002_03-D02-custom_tables", 3600)

# COMMAND ----------

# Step 3: preserve codelists used for present project

# dbutils.notebook.run("CCU002_03-D03-codelists", 3600)

# COMMAND ----------

#Step 4: create skinny table containing basic patient information [takes 1.5 hours to run]

# dbutils.notebook.run("CCU002_03-D04-patient_skinny_unassembled", 3600)

# COMMAND ----------

# Step 5: restrict skinny table using start date for follow-up of 1 January 2020

dbutils.notebook.run("CCU002_03-D05-patient_skinny_assembled", 3600)

# COMMAND ----------

# Step 6: perform quality assurance checks

dbutils.notebook.run("CCU002_03-D06-quality_assurance", 3600)

# COMMAND ----------

# Step 7: implement general inclusion and exclusion critera

dbutils.notebook.run("CCU002_03-D07-inclusion_exclusion", 3600)

# COMMAND ----------

# Step 8: define exposures

dbutils.notebook.run("CCU002_03-D08-exposures", 3600)

# COMMAND ----------

# Step 9: define covariates

dbutils.notebook.run("CCU002_03-D09-covariates", 3600)

# COMMAND ----------

# Step 10: define outcomes

dbutils.notebook.run("CCU002_03-D10-outcomes", 3600)

# COMMAND ----------

# Step 11: create analysis cohort

dbutils.notebook.run("CCU002_03-D11-cohort", 3600)
