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

# MAGIC %sql
# MAGIC -- Identify people with multiple records and mark as conflicted
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination_conflicted AS 
# MAGIC SELECT *
# MAGIC FROM (SELECT PERSON_ID_DEID, 
# MAGIC              CASE WHEN Records_per_Patient>1 THEN 1 ELSE 0 END AS conflicted_vax_record
# MAGIC       FROM (SELECT PERSON_ID_DEID, count(PERSON_ID_DEID) AS Records_per_Patient
# MAGIC             FROM global_temp.ccu002_03_vaccination_wide
# MAGIC             GROUP BY PERSON_ID_DEID))
# MAGIC WHERE conflicted_vax_record==1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restrict to one record per person with indicator for a conflicted record
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_vaccination AS 
# MAGIC SELECT vaccination.PERSON_ID_DEID AS NHS_NUMBER_DEID,
# MAGIC        vaccination.dose1_date AS vaccination_dose1_date,
# MAGIC        vaccination.dose1_product AS vaccination_dose1_product,
# MAGIC        vaccination.dose1_situation AS vaccination_dose1_situation,
# MAGIC        vaccination.dose2_date AS vaccination_dose2_date,
# MAGIC        vaccination.dose2_product AS vaccination_dose2_product,
# MAGIC        vaccination.dose2_situation AS vaccination_dose2_situation,
# MAGIC        conflict.conflicted_vax_record AS vaccination_conflicted
# MAGIC FROM (SELECT *
# MAGIC       FROM (SELECT *, row_number() OVER (PARTITION BY PERSON_ID_DEID ORDER BY dose1_date asc) AS record_number
# MAGIC             FROM (SELECT * 
# MAGIC                   FROM global_temp.ccu002_03_vaccination_wide))
# MAGIC       WHERE record_number=1) AS vaccination
# MAGIC LEFT JOIN global_temp.ccu002_03_vaccination_conflicted AS conflict ON conflict.PERSON_ID_DEID = vaccination.PERSON_ID_DEID

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

drop_table('ccu002_03_vaccination')
create_table('ccu002_03_vaccination')
