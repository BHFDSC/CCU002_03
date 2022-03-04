# Databricks notebook source
# MAGIC %md # CCU002_02-D14-table1_agedist
# MAGIC 
# MAGIC **Description** This notebook extracts the information needed for table 1.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

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

# MAGIC %md ## Make populations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW pfizer AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="Pfizer"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW pfizer_under40 AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="Pfizer"
# MAGIC AND cov_dose1_age<40

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW pfizer_40_69 AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="Pfizer"
# MAGIC AND cov_dose1_age>=40
# MAGIC AND cov_dose1_age<70

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW pfizer_70plus AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="Pfizer"
# MAGIC AND cov_dose1_age>=70

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW AstraZeneca AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="AstraZeneca"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW AstraZeneca_under40 AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="AstraZeneca"
# MAGIC AND cov_dose1_age<40

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW AstraZeneca_40_69 AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="AstraZeneca"
# MAGIC AND cov_dose1_age>=40
# MAGIC AND cov_dose1_age<70

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW AstraZeneca_70plus AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product=="AstraZeneca"
# MAGIC AND cov_dose1_age>=70

# COMMAND ----------

# MAGIC %md ## Generate output for each population

# COMMAND ----------

table = " AS category, COUNT(NHS_NUMBER_DEID) AS N, SUM(CASE WHEN cov_dose1_sex=1 THEN 1 ELSE 0 END) AS Sex_Male, SUM(CASE WHEN cov_dose2_sex=2 THEN 1 ELSE 0 END) AS Sex_Female, SUM(CASE WHEN cov_dose1_ethnicity='Asian or Asian British' THEN 1 ELSE 0 END) AS Ethnicity_Asian, SUM(CASE WHEN cov_dose1_ethnicity='Black or Black British' THEN 1 ELSE 0 END) AS Ethnicity_Black, SUM(CASE WHEN cov_dose1_ethnicity='Mixed' THEN 1 ELSE 0 END) AS Ethnicity_Mixed, SUM(CASE WHEN cov_dose1_ethnicity='Other Ethnic Groups' THEN 1 ELSE 0 END) AS Ethnicity_Other, SUM(CASE WHEN cov_dose1_ethnicity='White' THEN 1 ELSE 0 END) AS Ethnicity_White, SUM(CASE WHEN cov_dose1_ethnicity='Unknown' OR cov_dose1_ethnicity='missing' THEN 1 ELSE 0 END) AS Ethnicity_UnknownOrMissing, SUM(CASE WHEN cov_dose1_deprivation='Deciles_1_2' THEN 1 ELSE 0 END) AS Deprivation_1_2, SUM(CASE WHEN cov_dose1_deprivation='Deciles_3_4' THEN 1 ELSE 0 END) AS Deprivation_3_4, SUM(CASE WHEN cov_dose1_deprivation='Deciles_5_6' THEN 1 ELSE 0 END) AS Deprivation_5_6, SUM(CASE WHEN cov_dose1_deprivation='Deciles_7_8' THEN 1 ELSE 0 END) AS Deprivation_7_8, SUM(CASE WHEN cov_dose1_deprivation='Deciles_9_10' THEN 1 ELSE 0 END) AS Deprivation_9_10, SUM(CASE WHEN cov_dose1_prior_covid19=1 THEN 1 ELSE 0 END) AS MedicalHistory_CoronavirusInfection, SUM(CASE WHEN cov_dose1_myo_or_pericarditis=1 THEN 1 ELSE 0 END) AS MedicalHistory_Myopericarditis, SUM(CASE WHEN cov_dose1_region='North West' THEN 1 ELSE 0 END) AS Region_NorthWest, SUM(CASE WHEN cov_dose1_region IS NULL THEN 1 ELSE 0 END) AS Region_Missing, SUM(CASE WHEN cov_dose1_region='London' THEN 1 ELSE 0 END) AS Region_London, SUM(CASE WHEN cov_dose1_region='South East' THEN 1 ELSE 0 END) AS Region_SouthEast, SUM(CASE WHEN cov_dose1_region='East of England' THEN 1 ELSE 0 END) AS Region_EastOfEngland, SUM(CASE WHEN cov_dose1_region='South West' THEN 1 ELSE 0 END) AS Region_SouthWest, SUM(CASE WHEN cov_dose1_region='East Midlands' THEN 1 ELSE 0 END) AS Region_EastMidlands, SUM(CASE WHEN cov_dose1_region='Yorkshire and The Humber' THEN 1 ELSE 0 END) AS Region_YorkshireAndTheHumber, SUM(CASE WHEN cov_dose1_region='West Midlands' THEN 1 ELSE 0 END) AS Region_WestMidlands, SUM(CASE WHEN cov_dose1_region='North East' THEN 1 ELSE 0 END) AS Region_NorthEast FROM global_temp."

for data in ["Pfizer","Pfizer_under40","Pfizer_40_69","Pfizer_70plus","AstraZeneca","AstraZeneca_under40","AstraZeneca_40_69","AstraZeneca_70plus"]:
  sql("CREATE OR REPLACE GLOBAL TEMP VIEW t1_" + data + " AS SELECT '" + data + "_all'" + table + data)

# COMMAND ----------

# MAGIC %md ## Join output for all populations together

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_t1 AS
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_Pfizer
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_Pfizer_under40
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_Pfizer_40_69
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_Pfizer_70plus
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_AstraZeneca
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_AstraZeneca_under40
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_AstraZeneca_40_69
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_AstraZeneca_70plus

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

drop_table('ccu002_03_t1')
create_table('ccu002_03_t1')

# COMMAND ----------

# MAGIC %md ## Export

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_t1

# COMMAND ----------

# MAGIC %md # Supporting age distribution tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CASE WHEN cov_dose1_age>=100 THEN '100+' ELSE cov_dose1_age END) AS age,
# MAGIC        SUM(CASE WHEN vaccination_dose1_product = 'Pfizer' THEN 1 ELSE 0 END) AS dose1_Pfizer,
# MAGIC        SUM(CASE WHEN vaccination_dose1_product = 'AstraZeneca' THEN 1 ELSE 0 END) AS dose1_AstraZeneca,
# MAGIC        SUM(CASE WHEN vaccination_dose1_product IS NOT NULL THEN 1 ELSE 0 END) AS dose1_Total
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC GROUP BY age

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CASE WHEN cov_dose1_age>=100 THEN '100+' ELSE cov_dose1_age END) AS age,
# MAGIC        SUM(CASE WHEN vaccination_dose2_product = 'Pfizer' THEN 1 ELSE 0 END) AS dose2_Pfizer,
# MAGIC        SUM(CASE WHEN vaccination_dose1_product = 'Pfizer' THEN 1 ELSE 0 END) AS dose2_Total
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product = 'Pfizer'
# MAGIC GROUP BY age

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (CASE WHEN cov_dose1_age>=100 THEN '100+' ELSE cov_dose1_age END) AS age,
# MAGIC        SUM(CASE WHEN vaccination_dose2_product = 'AstraZeneca' THEN 1 ELSE 0 END) AS dose2_AstraZeneca,
# MAGIC        SUM(CASE WHEN vaccination_dose1_product = 'AstraZeneca' THEN 1 ELSE 0 END) AS dose2_Total
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC WHERE vaccination_dose1_product = 'AstraZeneca'
# MAGIC GROUP BY age
