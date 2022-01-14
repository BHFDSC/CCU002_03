# Databricks notebook source
# MAGIC %md %md # CCU002_03-D13-cohort
# MAGIC 
# MAGIC **Description** This notebook makes the analysis dataset.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker, Sam Ip, Spencer Keene

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

# MAGIC %md ## Combine cohort variables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cohort_full AS
# MAGIC SELECT FLOOR(RAND()*7)+1 AS CHUNK, -- CHUNK divides the data into parts for import into R
# MAGIC        cohort.NHS_NUMBER_DEID,
# MAGIC        vaccination.vaccination_dose1_date,
# MAGIC        vaccination.vaccination_dose1_product,
# MAGIC        vaccination.vaccination_dose2_date,
# MAGIC        vaccination.vaccination_dose2_product,
# MAGIC        FLOOR(DATEDIFF('2020-12-08', cohort.DATE_OF_BIRTH)/365.25) AS cov_dose1_age,
# MAGIC        cohort.SEX AS cov_dose1_sex,
# MAGIC        CASE WHEN cohort.CATEGORISED_ETHNICITY IS NULL THEN 'missing' ELSE cohort.CATEGORISED_ETHNICITY END AS cov_dose1_ethnicity,
# MAGIC        cov_dose1_lsoa.cov_dose1_lsoa,
# MAGIC        cov_dose1_region.cov_dose1_region,
# MAGIC        cov_dose1_deprivation.cov_dose1_deprivation,
# MAGIC        CASE WHEN (cov_dose1_pericarditis.cov_dose1_pericarditis=1 OR cov_dose1_myocarditis.cov_dose1_myocarditis=1) THEN 1 ELSE 0 END AS cov_dose1_myo_or_pericarditis,
# MAGIC        CASE WHEN (cov_dose1_prior_covid19.cov_dose1_prior_covid19=1) THEN 1 ELSE 0 END AS cov_dose1_prior_covid19,
# MAGIC        FLOOR(DATEDIFF(vaccination.vaccination_dose1_date, cohort.DATE_OF_BIRTH)/365.25) AS cov_dose2_age,
# MAGIC        cohort.SEX AS cov_dose2_sex,
# MAGIC        CASE WHEN cohort.CATEGORISED_ETHNICITY IS NULL THEN 'missing' ELSE cohort.CATEGORISED_ETHNICITY END AS cov_dose2_ethnicity,
# MAGIC        cov_dose2_lsoa.cov_dose2_lsoa,
# MAGIC        cov_dose2_region.cov_dose2_region,
# MAGIC        cov_dose2_deprivation.cov_dose2_deprivation,
# MAGIC        CASE WHEN (cov_dose2_pericarditis.cov_dose2_pericarditis=1 OR cov_dose2_myocarditis.cov_dose2_myocarditis=1) THEN 1 ELSE 0 END AS cov_dose2_myo_or_pericarditis,
# MAGIC        CASE WHEN (cov_dose2_prior_covid19.cov_dose2_prior_covid19=1) THEN 1 ELSE 0 END AS cov_dose2_prior_covid19,
# MAGIC         cohort.DATE_OF_DEATH AS out_death,
# MAGIC        CASE WHEN out_dose1_any_myocarditis.out_dose1_any_myocarditis<=out_dose1_any_pericarditis.out_dose1_any_pericarditis 
# MAGIC              AND out_dose1_any_myocarditis.out_dose1_any_myocarditis IS NOT NULL AND out_dose1_any_pericarditis.out_dose1_any_pericarditis IS NOT NULL THEN out_dose1_any_myocarditis
# MAGIC             WHEN out_dose1_any_myocarditis.out_dose1_any_myocarditis>out_dose1_any_pericarditis.out_dose1_any_pericarditis 
# MAGIC              AND out_dose1_any_myocarditis.out_dose1_any_myocarditis IS NOT NULL AND out_dose1_any_pericarditis.out_dose1_any_pericarditis IS NOT NULL THEN out_dose1_any_pericarditis
# MAGIC             WHEN out_dose1_any_myocarditis.out_dose1_any_myocarditis IS NOT NULL AND out_dose1_any_pericarditis.out_dose1_any_pericarditis IS NULL THEN out_dose1_any_myocarditis
# MAGIC             WHEN out_dose1_any_myocarditis.out_dose1_any_myocarditis IS NULL AND out_dose1_any_pericarditis.out_dose1_any_pericarditis IS NOT NULL THEN out_dose1_any_pericarditis
# MAGIC             ELSE NULL END AS out_dose1_any_myo_or_pericarditis,
# MAGIC        CASE WHEN out_dose2_any_myocarditis.out_dose2_any_myocarditis<=out_dose2_any_pericarditis.out_dose2_any_pericarditis 
# MAGIC              AND out_dose2_any_myocarditis.out_dose2_any_myocarditis IS NOT NULL AND out_dose2_any_pericarditis.out_dose2_any_pericarditis IS NOT NULL THEN out_dose2_any_myocarditis
# MAGIC             WHEN out_dose2_any_myocarditis.out_dose2_any_myocarditis>out_dose2_any_pericarditis.out_dose2_any_pericarditis 
# MAGIC              AND out_dose2_any_myocarditis.out_dose2_any_myocarditis IS NOT NULL AND out_dose2_any_pericarditis.out_dose2_any_pericarditis IS NOT NULL THEN out_dose2_any_pericarditis
# MAGIC             WHEN out_dose2_any_myocarditis.out_dose2_any_myocarditis IS NOT NULL AND out_dose2_any_pericarditis.out_dose2_any_pericarditis IS NULL THEN out_dose2_any_myocarditis
# MAGIC             WHEN out_dose2_any_myocarditis.out_dose2_any_myocarditis IS NULL AND out_dose2_any_pericarditis.out_dose2_any_pericarditis IS NOT NULL THEN out_dose2_any_pericarditis
# MAGIC             ELSE NULL END AS out_dose2_any_myo_or_pericarditis  
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_included_patients AS cohort
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination ON cohort.NHS_NUMBER_DEID = vaccination.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_out_dose1_any_myocarditis AS out_dose1_any_myocarditis ON cohort.NHS_NUMBER_DEID = out_dose1_any_myocarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_out_dose1_any_pericarditis AS out_dose1_any_pericarditis ON cohort.NHS_NUMBER_DEID = out_dose1_any_pericarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_out_dose2_any_myocarditis AS out_dose2_any_myocarditis ON cohort.NHS_NUMBER_DEID = out_dose2_any_myocarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_out_dose2_any_pericarditis AS out_dose2_any_pericarditis ON cohort.NHS_NUMBER_DEID = out_dose2_any_pericarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose1_lsoa AS cov_dose1_lsoa ON cohort.NHS_NUMBER_DEID = cov_dose1_lsoa.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose1_region AS cov_dose1_region ON cov_dose1_lsoa.cov_dose1_lsoa = cov_dose1_region.LSOA
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose1_deprivation AS cov_dose1_deprivation ON cov_dose1_lsoa.cov_dose1_lsoa = cov_dose1_deprivation.LSOA
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose1_pericarditis AS cov_dose1_pericarditis ON cohort.NHS_NUMBER_DEID = cov_dose1_pericarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose1_myocarditis AS cov_dose1_myocarditis ON cohort.NHS_NUMBER_DEID = cov_dose1_myocarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose1_prior_covid19 AS cov_dose1_prior_covid19 ON cohort.NHS_NUMBER_DEID = cov_dose1_prior_covid19.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose2_lsoa AS cov_dose2_lsoa ON cohort.NHS_NUMBER_DEID = cov_dose2_lsoa.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose2_region AS cov_dose2_region ON cov_dose2_lsoa.cov_dose2_lsoa = cov_dose2_region.LSOA
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose2_deprivation AS cov_dose2_deprivation ON cov_dose2_lsoa.cov_dose2_lsoa = cov_dose2_deprivation.LSOA
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose2_pericarditis AS cov_dose2_pericarditis ON cohort.NHS_NUMBER_DEID = cov_dose2_pericarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose2_myocarditis AS cov_dose2_myocarditis ON cohort.NHS_NUMBER_DEID = cov_dose2_myocarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_dose2_prior_covid19 AS cov_dose2_prior_covid19 ON cohort.NHS_NUMBER_DEID = cov_dose2_prior_covid19.NHS_NUMBER_DEID

# COMMAND ----------

# MAGIC %md ## Apply exclusions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cohort AS
# MAGIC SELECT *
# MAGIC FROM global_temp.ccu002_03_cohort_full
# MAGIC WHERE ((cov_dose1_sex=1) OR (cov_dose1_sex=2)) -- Keep males and females only
# MAGIC AND (cov_dose1_age<111) -- Remove people older than 110
# MAGIC AND ((vaccination_dose1_date IS NULL) OR ((vaccination_dose1_date IS NOT NULL) AND (vaccination_dose1_date>='2020-12-08'))) -- Remove people vaccinated before the UK vaccination program began
# MAGIC AND ((out_death IS NULL) OR ((out_death IS NOT NULL) AND (out_death>='2020-12-08'))) -- Remove people who died before the UK vaccination program began
# MAGIC AND ((cov_dose1_lsoa IS NULL) OR (LEFT(cov_dose1_lsoa,1)=="E")) -- Remove people not from England
# MAGIC AND ((cov_dose2_lsoa IS NULL) OR (LEFT(cov_dose2_lsoa,1)=="E")) -- Remove people not from England

# COMMAND ----------

# MAGIC %md ## Save cohort

# COMMAND ----------

drop_table('ccu002_03_cohort')

# COMMAND ----------

create_table('ccu002_03_cohort')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cohort description for ESCROW to share with SAIL
# MAGIC DESCRIBE dars_nic_391419_j3w9t_collab.ccu002_03_cohort
