# Databricks notebook source
# MAGIC %md # CCU002_03-D08-exposures
# MAGIC  
# MAGIC **Description** This notebook determines exposures for the analysis.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

# COMMAND ----------

# MAGIC %md ## Clear cache

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE

# COMMAND ----------

# MAGIC %md  ## Define functions

# COMMAND ----------

# Define create table function by Sam Hollings
# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md ## Create annotated vaccination table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination_raw AS
# MAGIC SELECT vaccination_raw.PERSON_ID_DEID,
# MAGIC        to_date(cast(vaccination_raw.RECORDED_DATE as string), 'yyyyMMdd') AS RECORDED_DATE,
# MAGIC        REPLACE(product.name, 'COVID19_vaccine_', '') AS VACCINE_PRODUCT,
# MAGIC        REPLACE(procedure.name,'COVID19_vaccine_','') AS VACCINATION_PROCEDURE,
# MAGIC        REPLACE(situation.name,'COVID19_vaccine_','') AS VACCINATION_SITUATION_CODE
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccine_status_dars_nic_391419_j3w9t AS vaccination_raw
# MAGIC LEFT JOIN (SELECT code, name FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE left(name,16)="COVID19_vaccine_") AS product ON vaccination_raw.VACCINE_PRODUCT_CODE = product.code
# MAGIC LEFT JOIN (SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE left(name,16)="COVID19_vaccine_") AS procedure ON vaccination_raw.VACCINATION_PROCEDURE_CODE = procedure.code
# MAGIC LEFT JOIN (SELECT code, name FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE left(name,16)="COVID19_vaccine_") AS situation ON vaccination_raw.VACCINATION_SITUATION_CODE = situation.code

# COMMAND ----------

# MAGIC %md ## Convert vaccination data to wide format

# COMMAND ----------

# Create dose specific vaccination tables
for dose in ["dose1","dose2"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination_" + dose + " AS SELECT PERSON_ID_DEID, RECORDED_DATE AS " + dose + "_date, VACCINE_PRODUCT AS " + dose + "_product, VACCINATION_SITUATION_CODE AS " + dose + "_situation FROM global_temp.ccu002_03_vaccination_raw WHERE VACCINATION_PROCEDURE='" + dose + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join dose specific vaccination tables to create wide format vaccination table
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination_wide AS
# MAGIC SELECT dose1.PERSON_ID_DEID,
# MAGIC        dose1.dose1_date,
# MAGIC        dose1.dose1_product,
# MAGIC        dose1.dose1_situation,
# MAGIC        dose2.dose2_date,
# MAGIC        dose2.dose2_product,
# MAGIC        dose2.dose2_situation
# MAGIC FROM global_temp.ccu002_03_vaccination_dose1 AS dose1
# MAGIC FULL JOIN global_temp.ccu002_03_vaccination_dose2 AS dose2 on dose1.PERSON_ID_DEID = dose2.PERSON_ID_DEID

# COMMAND ----------

# MAGIC %md ## Impose vaccine based restrictions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination_intermediate AS
# MAGIC SELECT DISTINCT *
# MAGIC FROM global_temp.ccu002_03_vaccination_wide
# MAGIC WHERE PERSON_ID_DEID IS NOT NULL
# MAGIC AND ((dose1_date>'2020-12-07') AND (dose1_date IS NOT NULL)) -- remove individuals vacccinated before start of vaccine program or missing a first dose
# MAGIC AND ((dose1_date < dose2_date) OR (dose2_date IS NULL)) -- remove individuals whose second dose is before their first dose
# MAGIC AND ((DATEDIFF(dose2_date, dose1_date)>=21) OR (dose2_date IS NULL)) -- remove individuals whose second dose is less than 3 weeks after their first dose
# MAGIC AND ((dose1_situation IS NULL) AND (dose2_situation IS NULL)) -- remove individuals with a situation (4634 individuals)
# MAGIC AND ((dose1_product=dose2_product) OR (dose1_product!=dose2_product AND dose2_date>'2021-05-07')) -- remove individuals with mixed vaccine products before 7 May 2021

# COMMAND ----------

# MAGIC %md ## Restrict to individuals with a single record

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination AS
# MAGIC SELECT PERSON_ID_DEID AS NHS_NUMBER_DEID,
# MAGIC        dose1_date AS vaccination_dose1_date,
# MAGIC        dose1_product AS vaccination_dose1_product,
# MAGIC        dose2_date AS vaccination_dose2_date,
# MAGIC        dose2_product AS vaccination_dose2_product
# MAGIC FROM global_temp.ccu002_03_vaccination_intermediate
# MAGIC WHERE PERSON_ID_DEID IN (SELECT PERSON_ID_DEID
# MAGIC                          FROM (SELECT count(PERSON_ID_DEID) AS Records_per_Patient, PERSON_ID_DEID
# MAGIC                                FROM global_temp.ccu002_03_vaccination_intermediate
# MAGIC                                GROUP BY PERSON_ID_DEID)
# MAGIC                          WHERE Records_per_Patient==1)

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

drop_table('ccu002_03_vaccination')
create_table('ccu002_03_vaccination')
