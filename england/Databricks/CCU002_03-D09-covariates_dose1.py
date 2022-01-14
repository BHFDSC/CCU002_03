# Databricks notebook source
# MAGIC %md # CCU002_03-D09-covariates_dose1
# MAGIC  
# MAGIC **Description** This notebook extracts the covariates for the analysis.
# MAGIC 
# MAGIC **Author(s)** Sam Ip, Spencer Keene, Rochelle Knight, Venexia Walker

# COMMAND ----------

# MAGIC %md ## Clear cache

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE

# COMMAND ----------

# MAGIC %md ## Define functions

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

# MAGIC %md ## Define parameters

# COMMAND ----------

index_date = '2020-12-08'

# COMMAND ----------

# MAGIC %md ## Medical history

# COMMAND ----------

medhistory = ['myocarditis','pericarditis']

# COMMAND ----------

# MAGIC %md ### HES APC

# COMMAND ----------

for codelist in medhistory:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_hesapc_" + codelist + " AS SELECT DISTINCT NHS_NUMBER_DEID FROM (SELECT NHS_NUMBER_DEID, EPISTART FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_longformat WHERE CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' AND TERMINOLOGY='ICD10')) WHERE EPISTART<'" + index_date + "'")

# COMMAND ----------

# MAGIC %md ### SUS

# COMMAND ----------

for codelist in medhistory:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_sus_" + codelist + " AS SELECT DISTINCT NHS_NUMBER_DEID FROM (SELECT NHS_NUMBER_DEID, EPISODE_START_DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_sus_longformat WHERE ((CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')) OR (LEFT(CODE,3) IN (SELECT code FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists WHERE name = '" + codelist + "' and terminology=='ICD10')))) WHERE EPISODE_START_DATE<'" + index_date + "'")

# COMMAND ----------

# MAGIC %md ### Combined

# COMMAND ----------

for codelist in medhistory:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_" + codelist + " AS SELECT DISTINCT NHS_NUMBER_DEID, 1 AS cov_dose1_" + codelist + " FROM (SELECT NHS_NUMBER_DEID FROM global_temp.ccu002_03_cov_dose1_hesapc_" + codelist + " UNION ALL SELECT NHS_NUMBER_DEID FROM global_temp.ccu002_03_cov_dose1_sus_" + codelist + ") GROUP BY NHS_NUMBER_DEID")

# COMMAND ----------

# MAGIC %md ## COVID19 infection

# COMMAND ----------

sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_prior_covid19 AS SELECT DISTINCT person_id_deid AS NHS_NUMBER_DEID, 1 AS cov_dose1_prior_covid19 FROM (SELECT data.person_id_deid, data.date, vaccination.vaccination_dose1_date FROM dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination INNER JOIN (SELECT person_id_deid, date FROM dars_nic_391419_j3w9t_collab.ccu002_03_ccu013_covid_trajectory WHERE covid_status='confirmed') AS data ON vaccination.NHS_NUMBER_DEID = data.person_id_deid) WHERE date<'" + index_date + "' AND date>='2020-01-01'")

# COMMAND ----------

# MAGIC %md ## LSOA  

# COMMAND ----------

sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_lsoa_nondistinct AS SELECT DISTINCT NHS_NUMBER_DEID, LSOA FROM (SELECT DISTINCT NHS_NUMBER_DEID, LSOA, DATE FROM dars_nic_391419_j3w9t_collab.ccu002_03_gdppr_dars_nic_391419_j3w9t) WHERE DATE<'" + index_date + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_lsoa AS
# MAGIC SELECT DISTINCT NHS_NUMBER_DEID, LSOA AS cov_dose1_lsoa
# MAGIC FROM global_temp.ccu002_03_cov_dose1_lsoa_nondistinct
# MAGIC WHERE NHS_NUMBER_DEID IN (SELECT NHS_NUMBER_DEID
# MAGIC                           FROM (SELECT count(NHS_NUMBER_DEID) AS Records_per_Patient, NHS_NUMBER_DEID
# MAGIC                                 FROM global_temp.ccu002_03_cov_dose1_lsoa_nondistinct
# MAGIC                                 GROUP BY NHS_NUMBER_DEID)
# MAGIC                           WHERE Records_per_Patient = 1)

# COMMAND ----------

# MAGIC %md ## Region name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_region AS
# MAGIC SELECT DISTINCT lsoa_code AS LSOA, region_name AS cov_dose1_region
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_lsoa_region_lookup

# COMMAND ----------

# MAGIC %md ## Index of Multiple Deprivation

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cov_dose1_deprivation AS
# MAGIC SELECT LSOA_CODE_2011 AS LSOA,
# MAGIC        CASE WHEN DECI_IMD IS NULL THEN 'missing' 
# MAGIC             WHEN DECI_IMD=1 OR DECI_IMD=2 THEN 'Deciles_1_2'
# MAGIC             WHEN DECI_IMD=3 OR DECI_IMD=4 THEN 'Deciles_3_4'
# MAGIC             WHEN DECI_IMD=5 OR DECI_IMD=6 THEN 'Deciles_5_6'
# MAGIC             WHEN DECI_IMD=7 OR DECI_IMD=8 THEN 'Deciles_7_8'
# MAGIC             WHEN DECI_IMD=9 OR DECI_IMD=10 THEN 'Deciles_9_10' END AS cov_dose1_deprivation
# MAGIC FROM (SELECT DISTINCT LSOA_CODE_2011, DECI_IMD
# MAGIC       FROM dss_corporate.english_indices_of_dep_v02
# MAGIC       WHERE LSOA_CODE_2011 IN (SELECT LSOA FROM dars_nic_391419_j3w9t_collab.ccu002_03_gdppr_dars_nic_391419_j3w9t)
# MAGIC         AND LSOA_CODE_2011 IS NOT NULL
# MAGIC         AND IMD IS NOT NULL
# MAGIC         AND IMD_YEAR = '2019')

# COMMAND ----------

# MAGIC %md ## Save as tables

# COMMAND ----------

covariates = ["myocarditis","pericarditis","prior_covid19","lsoa","region","deprivation"]

# COMMAND ----------

for codelist in covariates:
  drop_table('ccu002_03_cov_dose1_'+codelist)

# COMMAND ----------

for codelist in covariates:
  create_table('ccu002_03_cov_dose1_'+codelist)
