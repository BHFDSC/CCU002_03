# Databricks notebook source
# MAGIC %md # CCU002_03-D11-outcomes_dose1
# MAGIC  
# MAGIC **Description** This notebook determines outcomes for the analysis.
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

# MAGIC %md ## Specify outcomes

# COMMAND ----------

outcomes = ['myocarditis','pericarditis']

# COMMAND ----------

index_date = '2020-12-08'

# COMMAND ----------

# MAGIC %md ## GDPPR

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_gdppr_" + codelist + " AS SELECT NHS_NUMBER_DEID, min(DATE) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_gdppr_dars_nic_391419_j3w9t WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND terminology=='SNOMED')) WHERE DATE>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## HES APC

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_first_hesapc_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISTART) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, EPISTART FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_longformat WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND TERMINOLOGY='ICD10') AND (SOURCE='DIAG_3_01' OR SOURCE='DIAG_4_01')) WHERE EPISTART>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_any_hesapc_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISTART) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, EPISTART FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_longformat WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND TERMINOLOGY='ICD10')) WHERE EPISTART>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## SUS

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_first_sus_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISODE_START_DATE) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, EPISODE_START_DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_sus_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10'))) AND SOURCE='PRIMARY_DIAGNOSIS_CODE') WHERE EPISODE_START_DATE>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_any_sus_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISODE_START_DATE) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, EPISODE_START_DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_sus_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')))) WHERE EPISODE_START_DATE>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## Deaths

# COMMAND ----------

# DEATHS
for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_first_deaths_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(DATE) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_deaths_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10'))) AND SOURCE='S_UNDERLYING_COD_ICD10') WHERE DATE>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# DEATHS
for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_any_deaths_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(DATE) AS out_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_deaths_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')))) WHERE DATE>='" + index_date + "' GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## Combine data sources 

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_first_" + codelist + " AS SELECT NHS_NUMBER_DEID, min(out_dose1_" + codelist + ") AS out_dose1_first_" + codelist + " FROM (SELECT * FROM global_temp.ccu002_03_out_dose1_gdppr_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_dose1_first_hesapc_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_dose1_first_deaths_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_dose1_first_sus_" + codelist + ") GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_dose1_any_" + codelist + " AS SELECT NHS_NUMBER_DEID, min(out_dose1_" + codelist + ") AS out_dose1_any_" + codelist + " FROM (SELECT * FROM global_temp.ccu002_03_out_dose1_gdppr_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_dose1_any_hesapc_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_dose1_any_deaths_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_dose1_any_sus_" + codelist + ") GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## Save as tables

# COMMAND ----------

for codelist in outcomes:
  drop_table('ccu002_03_out_dose1_first_'+codelist)

# COMMAND ----------

for codelist in outcomes:
  create_table('ccu002_03_out_dose1_first_'+codelist)

# COMMAND ----------

for codelist in outcomes:
  drop_table('ccu002_03_out_dose1_any_'+codelist)

# COMMAND ----------

for codelist in outcomes:
  create_table('ccu002_03_out_dose1_any_'+codelist)
