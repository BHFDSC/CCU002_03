# Databricks notebook source
# MAGIC %md %md # CCU002_03-D11-cohort
# MAGIC 
# MAGIC **Description** This notebook makes the analysis dataset.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker, Sam Ip, Spencer Keene

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

# MAGIC %md ## Combine basic cohort variables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cohort_full AS
# MAGIC SELECT FLOOR(RAND()*7)+1 AS CHUNK, -- CHUNK divides the data into parts for import into R
# MAGIC        cohort.NHS_NUMBER_DEID,
# MAGIC        vaccination.vaccination_dose1_date,
# MAGIC        vaccination.vaccination_dose1_product,
# MAGIC        vaccination.vaccination_dose2_date,
# MAGIC        vaccination.vaccination_dose2_product,
# MAGIC        FLOOR(DATEDIFF(vaccination.vaccination_dose1_date, cohort.DATE_OF_BIRTH)/365.25) AS cov_age,
# MAGIC        cohort.SEX AS cov_sex,
# MAGIC        CASE WHEN cohort.CATEGORISED_ETHNICITY IS NULL THEN 'missing' ELSE cohort.CATEGORISED_ETHNICITY END AS cov_ethnicity,
# MAGIC        cohort.DATE_OF_DEATH AS out_death,
# MAGIC        CASE WHEN out_any_myocarditis.out_any_myocarditis<=out_any_pericarditis.out_any_pericarditis 
# MAGIC              AND out_any_myocarditis.out_any_myocarditis IS NOT NULL AND out_any_pericarditis.out_any_pericarditis IS NOT NULL THEN out_any_myocarditis
# MAGIC             WHEN out_any_myocarditis.out_any_myocarditis>out_any_pericarditis.out_any_pericarditis 
# MAGIC              AND out_any_myocarditis.out_any_myocarditis IS NOT NULL AND out_any_pericarditis.out_any_pericarditis IS NOT NULL THEN out_any_pericarditis
# MAGIC             WHEN out_any_myocarditis.out_any_myocarditis IS NOT NULL AND out_any_pericarditis.out_any_pericarditis IS NULL THEN out_any_myocarditis
# MAGIC             WHEN out_any_myocarditis.out_any_myocarditis IS NULL AND out_any_pericarditis.out_any_pericarditis IS NOT NULL THEN out_any_pericarditis
# MAGIC             ELSE NULL END AS out_any_myo_or_pericarditis,
# MAGIC        cov_lsoa.LSOA,
# MAGIC        cov_region.cov_region,
# MAGIC        cov_deprivation.cov_deprivation,
# MAGIC        CASE WHEN (cov_pericarditis.cov_pericarditis=1 OR cov_myocarditis.cov_myocarditis=1) THEN 1 ELSE 0 END AS cov_myo_or_pericarditis,
# MAGIC        CASE WHEN (cov_prior_covid19=1) THEN 1 ELSE 0 END AS cov_prior_covid19
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_included_patients AS cohort
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_vaccination AS vaccination ON cohort.NHS_NUMBER_DEID = vaccination.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_out_any_myocarditis AS out_any_myocarditis ON cohort.NHS_NUMBER_DEID = out_any_myocarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_out_any_pericarditis AS out_any_pericarditis ON cohort.NHS_NUMBER_DEID = out_any_pericarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_lsoa AS cov_lsoa ON cohort.NHS_NUMBER_DEID = cov_lsoa.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_region AS cov_region ON cov_lsoa.LSOA = cov_region.LSOA
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_deprivation AS cov_deprivation ON cov_lsoa.LSOA = cov_deprivation.LSOA
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_pericarditis AS cov_pericarditis ON cohort.NHS_NUMBER_DEID = cov_pericarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_myocarditis AS cov_myocarditis ON cohort.NHS_NUMBER_DEID = cov_myocarditis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_03_cov_prior_covid19 AS cov_prior_covid19 ON cohort.NHS_NUMBER_DEID = cov_prior_covid19.NHS_NUMBER_DEID

# COMMAND ----------

# MAGIC %md ## Apply exclusions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_cohort AS
# MAGIC SELECT *
# MAGIC FROM global_temp.ccu002_03_cohort_full
# MAGIC WHERE vaccination_dose1_date IS NOT NULL -- Remove people who have not been vaccinated
# MAGIC AND vaccination_dose1_date>'2020-12-08' -- Remove people vaccinated before UK vaccination program began
# MAGIC AND ((out_death IS NULL) OR (vaccination_dose1_date <= out_death)) -- Remove people who die prior to vaccination
# MAGIC AND (cov_age<111) -- Remove people older than 110

# COMMAND ----------

# MAGIC %md ## Save cohort

# COMMAND ----------

drop_table('ccu002_03_cohort')

# COMMAND ----------

create_table('ccu002_03_cohort')
