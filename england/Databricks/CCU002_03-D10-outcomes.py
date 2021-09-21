# Databricks notebook source
# MAGIC %md # CCU002_03-D10-outcomes
# MAGIC  
# MAGIC **Description** This notebook determines outcomes for the analysis.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

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

outcomes = ['ICVT_pregnancy',
            'artery_dissect',
            'angina',
            'other_DVT',
            'DVT_ICVT',
            'DVT_pregnancy',
            'DVT_DVT',
            'fracture',
            'thrombophilia',
            'thrombocytopenia',
            'life_arrhythmia',
            'pericarditis',
            'TTP',
            'mesenteric_thrombus',
            'DIC',
            'myocarditis',
            'stroke_TIA',
            'stroke_isch',
            'other_arterial_embolism',
            'unstable_angina',
            'PE',
            'AMI',
            'HF',
            'portal_vein_thrombosis',
            'cardiomyopathy',
            'stroke_SAH_HS']

# COMMAND ----------

# Replace full outcome list with reduced outcome list for time being
outcomes = ['myocarditis','pericarditis']

# COMMAND ----------

# MAGIC %md ## GDPPR

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_gdppr_" + codelist + " AS SELECT NHS_NUMBER_DEID, min(DATE) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.DATE, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_gdppr_dars_nic_391419_j3w9t WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND terminology=='SNOMED')) AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE DATE>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## HES APC

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_first_hesapc_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISTART) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.EPISTART, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, EPISTART FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_longformat WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND TERMINOLOGY='ICD10') AND (SOURCE='DIAG_3_01' OR SOURCE='DIAG_4_01')) AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE EPISTART>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_any_hesapc_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISTART) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.EPISTART, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, EPISTART FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_longformat WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND TERMINOLOGY='ICD10')) AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE EPISTART>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## SUS

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_first_sus_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISODE_START_DATE) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.EPISODE_START_DATE, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, EPISODE_START_DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_sus_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10'))) AND SOURCE='PRIMARY_DIAGNOSIS_CODE') AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE EPISODE_START_DATE>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_any_sus_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(EPISODE_START_DATE) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.EPISODE_START_DATE, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, EPISODE_START_DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_sus_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')))) AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE EPISODE_START_DATE>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## Deaths

# COMMAND ----------

# DEATHS
for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_first_deaths_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(DATE) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.DATE, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_deaths_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10'))) AND SOURCE='S_UNDERLYING_COD_ICD10') AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE DATE>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# DEATHS
for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_any_deaths_" + codelist + " AS SELECT NHS_NUMBER_DEID, MIN(DATE) AS out_" + codelist + " FROM (SELECT data.NHS_NUMBER_DEID, data.DATE, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT NHS_NUMBER_DEID, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_deaths_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')))) AS data ON vaccination.NHS_NUMBER_DEID = data.NHS_NUMBER_DEID) WHERE DATE>=vaccination_dose1_date GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## Combine data sources 

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_first_" + codelist + " AS SELECT NHS_NUMBER_DEID, min(out_" + codelist + ") AS out_first_" + codelist + " FROM (SELECT * FROM global_temp.ccu002_03_out_gdppr_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_first_hesapc_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_first_deaths_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_first_sus_" + codelist + ") GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

for codelist in outcomes:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_out_any_" + codelist + " AS SELECT NHS_NUMBER_DEID, min(out_" + codelist + ") AS out_any_" + codelist + " FROM (SELECT * FROM global_temp.ccu002_03_out_gdppr_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_any_hesapc_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_any_deaths_" + codelist + " UNION ALL SELECT * FROM global_temp.ccu002_03_out_any_sus_" + codelist + ") GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## Save as tables

# COMMAND ----------

for codelist in outcomes:
  drop_table('ccu002_03_out_first_'+codelist)

# COMMAND ----------

for codelist in outcomes:
  create_table('ccu002_03_out_first_'+codelist)

# COMMAND ----------

for codelist in outcomes:
  drop_table('ccu002_03_out_any_'+codelist)

# COMMAND ----------

for codelist in outcomes:
  create_table('ccu002_03_out_any_'+codelist)
