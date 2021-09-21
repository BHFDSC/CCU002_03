# Databricks notebook source
# MAGIC %md # CCU002_03-D04-custom_tables
# MAGIC  
# MAGIC **Description** This notebook creates custom tables, such as long format HES.
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

# MAGIC %md ## Long format HES APC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_hes_apc_longformat AS
# MAGIC SELECT *
# MAGIC FROM (SELECT PERSON_ID_DEID AS NHS_NUMBER_DEID, 
# MAGIC              ADMIDATE, 
# MAGIC              DISDATE,
# MAGIC              EPISTART, 
# MAGIC              EPIEND,
# MAGIC              EPIKEY,
# MAGIC              STACK(40,
# MAGIC                    DIAG_3_01, 'DIAG_3_01',
# MAGIC                    DIAG_3_02, 'DIAG_3_02',
# MAGIC                    DIAG_3_03, 'DIAG_3_03',
# MAGIC                    DIAG_3_04, 'DIAG_3_04',
# MAGIC                    DIAG_3_05, 'DIAG_3_05',
# MAGIC                    DIAG_3_06, 'DIAG_3_06',
# MAGIC                    DIAG_3_07, 'DIAG_3_07',
# MAGIC                    DIAG_3_08, 'DIAG_3_08',
# MAGIC                    DIAG_3_09, 'DIAG_3_09',
# MAGIC                    DIAG_3_10, 'DIAG_3_10',
# MAGIC                    DIAG_3_11, 'DIAG_3_11',
# MAGIC                    DIAG_3_12, 'DIAG_3_12',
# MAGIC                    DIAG_3_13, 'DIAG_3_13',
# MAGIC                    DIAG_3_14, 'DIAG_3_14',
# MAGIC                    DIAG_3_15, 'DIAG_3_15',
# MAGIC                    DIAG_3_16, 'DIAG_3_16',
# MAGIC                    DIAG_3_17, 'DIAG_3_17',
# MAGIC                    DIAG_3_18, 'DIAG_3_18',
# MAGIC                    DIAG_3_19, 'DIAG_3_19',
# MAGIC                    DIAG_3_20, 'DIAG_3_20',
# MAGIC                    DIAG_4_01, 'DIAG_4_01',
# MAGIC                    DIAG_4_02, 'DIAG_4_02',
# MAGIC                    DIAG_4_03, 'DIAG_4_03',
# MAGIC                    DIAG_4_04, 'DIAG_4_04',
# MAGIC                    DIAG_4_05, 'DIAG_4_05',
# MAGIC                    DIAG_4_06, 'DIAG_4_06',
# MAGIC                    DIAG_4_07, 'DIAG_4_07',
# MAGIC                    DIAG_4_08, 'DIAG_4_08',
# MAGIC                    DIAG_4_09, 'DIAG_4_09',
# MAGIC                    DIAG_4_10, 'DIAG_4_10',
# MAGIC                    DIAG_4_11, 'DIAG_4_11',
# MAGIC                    DIAG_4_12, 'DIAG_4_12',
# MAGIC                    DIAG_4_13, 'DIAG_4_13',
# MAGIC                    DIAG_4_14, 'DIAG_4_14',
# MAGIC                    DIAG_4_15, 'DIAG_4_15',
# MAGIC                    DIAG_4_16, 'DIAG_4_16',
# MAGIC                    DIAG_4_17, 'DIAG_4_17',
# MAGIC                    DIAG_4_18, 'DIAG_4_18',
# MAGIC                    DIAG_4_19, 'DIAG_4_19',
# MAGIC                    DIAG_4_20, 'DIAG_4_20') AS (CODE, SOURCE)
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_all_years)
# MAGIC WHERE NHS_NUMBER_DEID IS NOT NULL AND CODE IS NOT NULL

# COMMAND ----------

drop_table('ccu002_03_hes_apc_longformat')
create_table('ccu002_03_hes_apc_longformat')

# COMMAND ----------

# MAGIC %md ## Long format HES APC surgery

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_hes_apc_opertn_longformat_tmp AS
# MAGIC SELECT *
# MAGIC FROM (SELECT PERSON_ID_DEID AS NHS_NUMBER_DEID, 
# MAGIC              ADMIDATE, 
# MAGIC              DISDATE,
# MAGIC              EPISTART, 
# MAGIC              EPIEND,
# MAGIC              EPIKEY,
# MAGIC              STACK(48,
# MAGIC                     OPERTN_3_01, 'OPERTN_3_01',
# MAGIC                     OPERTN_3_02, 'OPERTN_3_02',
# MAGIC                     OPERTN_3_03, 'OPERTN_3_03',
# MAGIC                     OPERTN_3_04, 'OPERTN_3_04',
# MAGIC                     OPERTN_3_05, 'OPERTN_3_05',
# MAGIC                     OPERTN_3_06, 'OPERTN_3_06',
# MAGIC                     OPERTN_3_07, 'OPERTN_3_07',
# MAGIC                     OPERTN_3_08, 'OPERTN_3_08',
# MAGIC                     OPERTN_3_09, 'OPERTN_3_09',
# MAGIC                     OPERTN_3_10, 'OPERTN_3_10',
# MAGIC                     OPERTN_3_11, 'OPERTN_3_11',
# MAGIC                     OPERTN_3_12, 'OPERTN_3_12',
# MAGIC                     OPERTN_3_13, 'OPERTN_3_13',
# MAGIC                     OPERTN_3_14, 'OPERTN_3_14',
# MAGIC                     OPERTN_3_15, 'OPERTN_3_15',
# MAGIC                     OPERTN_3_16, 'OPERTN_3_16',
# MAGIC                     OPERTN_3_17, 'OPERTN_3_17',
# MAGIC                     OPERTN_3_18, 'OPERTN_3_18',
# MAGIC                     OPERTN_3_19, 'OPERTN_3_19',
# MAGIC                     OPERTN_3_20, 'OPERTN_3_20',
# MAGIC                     OPERTN_3_21, 'OPERTN_3_21',
# MAGIC                     OPERTN_3_22, 'OPERTN_3_22',
# MAGIC                     OPERTN_3_23, 'OPERTN_3_23',
# MAGIC                     OPERTN_3_24, 'OPERTN_3_24',
# MAGIC                     OPERTN_4_01, 'OPERTN_4_01',
# MAGIC                     OPERTN_4_02, 'OPERTN_4_02',
# MAGIC                     OPERTN_4_03, 'OPERTN_4_03',
# MAGIC                     OPERTN_4_04, 'OPERTN_4_04',
# MAGIC                     OPERTN_4_05, 'OPERTN_4_05',
# MAGIC                     OPERTN_4_06, 'OPERTN_4_06',
# MAGIC                     OPERTN_4_07, 'OPERTN_4_07',
# MAGIC                     OPERTN_4_08, 'OPERTN_4_08',
# MAGIC                     OPERTN_4_09, 'OPERTN_4_09',
# MAGIC                     OPERTN_4_10, 'OPERTN_4_10',
# MAGIC                     OPERTN_4_11, 'OPERTN_4_11',
# MAGIC                     OPERTN_4_12, 'OPERTN_4_12',
# MAGIC                     OPERTN_4_13, 'OPERTN_4_13',
# MAGIC                     OPERTN_4_14, 'OPERTN_4_14',
# MAGIC                     OPERTN_4_15, 'OPERTN_4_15',
# MAGIC                     OPERTN_4_16, 'OPERTN_4_16',
# MAGIC                     OPERTN_4_17, 'OPERTN_4_17',
# MAGIC                     OPERTN_4_18, 'OPERTN_4_18',
# MAGIC                     OPERTN_4_19, 'OPERTN_4_19',
# MAGIC                     OPERTN_4_20, 'OPERTN_4_20',
# MAGIC                     OPERTN_4_21, 'OPERTN_4_21',
# MAGIC                     OPERTN_4_22, 'OPERTN_4_22',
# MAGIC                     OPERTN_4_23, 'OPERTN_4_23',
# MAGIC                     OPERTN_4_24, 'OPERTN_4_24') AS (CODE, SOURCE)
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_03_hes_apc_all_years)
# MAGIC WHERE NHS_NUMBER_DEID IS NOT NULL

# COMMAND ----------

drop_table('ccu002_03_hes_apc_opertn_longformat')
create_table('ccu002_03_hes_apc_opertn_longformat')

# COMMAND ----------

# MAGIC %md ## Long format deaths

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_deaths_longformat AS
# MAGIC SELECT *
# MAGIC FROM (SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID AS NHS_NUMBER_DEID, 
# MAGIC        to_date(cast(REG_DATE_OF_DEATH as string), 'yyyyMMdd') AS DATE,
# MAGIC        STACK(16,
# MAGIC              S_UNDERLYING_COD_ICD10,'S_UNDERLYING_COD_ICD10',
# MAGIC              S_COD_CODE_1, 'S_COD_CODE_1', 
# MAGIC              S_COD_CODE_2, 'S_COD_CODE_2', 
# MAGIC              S_COD_CODE_3, 'S_COD_CODE_3', 
# MAGIC              S_COD_CODE_4, 'S_COD_CODE_4', 
# MAGIC              S_COD_CODE_5, 'S_COD_CODE_5', 
# MAGIC              S_COD_CODE_6, 'S_COD_CODE_6', 
# MAGIC              S_COD_CODE_7, 'S_COD_CODE_7', 
# MAGIC              S_COD_CODE_8, 'S_COD_CODE_8', 
# MAGIC              S_COD_CODE_9, 'S_COD_CODE_9', 
# MAGIC              S_COD_CODE_10, 'S_COD_CODE_10', 
# MAGIC              S_COD_CODE_11, 'S_COD_CODE_11', 
# MAGIC              S_COD_CODE_12, 'S_COD_CODE_12', 
# MAGIC              S_COD_CODE_13, 'S_COD_CODE_13', 
# MAGIC              S_COD_CODE_14, 'S_COD_CODE_14', 
# MAGIC              S_COD_CODE_15, 'S_COD_CODE_15') AS (CODE, SOURCE)
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_deaths_dars_nic_391419_j3w9t)
# MAGIC WHERE NHS_NUMBER_DEID IS NOT NULL AND DATE IS NOT NULL AND CODE IS NOT NULL

# COMMAND ----------

drop_table('ccu002_03_deaths_longformat')
create_table('ccu002_03_deaths_longformat')

# COMMAND ----------

# MAGIC %md ## Long format SUS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_sus_longformat AS
# MAGIC SELECT NHS_NUMBER_DEID, 
# MAGIC        START_DATE_HOSPITAL_PROVIDER_SPELL, 
# MAGIC        END_DATE_HOSPITAL_PROVIDER_SPELL, 
# MAGIC        EPISODE_START_DATE, 
# MAGIC        EPISODE_END_DATE, 
# MAGIC        EPISODE_DURATION, 
# MAGIC        EPISODE_NUMBER,
# MAGIC        STACK(25,
# MAGIC              LEFT(REGEXP_REPLACE(PRIMARY_DIAGNOSIS_CODE,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'PRIMARY_DIAGNOSIS_CODE', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_1,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_1', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_2,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_2', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_3,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_3', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_4,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_4', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_5,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_5', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_6,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_6', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_7,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_7', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_8,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_8', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_9,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_9', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_10,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_10', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_11,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_11',
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_12,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_12', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_13,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_13', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_14,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_14', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_15,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_15', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_16,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_16', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_17,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_17',
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_18,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_18', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_19,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_19', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_20,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_20',
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_21,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_21', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_22,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_22', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_23,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_23', 
# MAGIC              LEFT(REGEXP_REPLACE(SECONDARY_DIAGNOSIS_CODE_24,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4), 'SECONDARY_DIAGNOSIS_CODE_24') AS (CODE,SOURCE)
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_sus_dars_nic_391419_j3w9t

# COMMAND ----------

drop_table('ccu002_03_sus_longformat')
create_table('ccu002_03_sus_longformat')

# COMMAND ----------

# MAGIC %md ##LSOA region lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Code developed by Sam Hollings and Spencer Keene
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_lsoa_region_lookup AS
# MAGIC with 
# MAGIC curren_chd_geo_listings as (SELECT * FROM dss_corporate.ons_chd_geo_listings WHERE IS_CURRENT = 1),
# MAGIC lsoa_auth as (
# MAGIC   SELECT e01.geography_code as lsoa_code, e01.geography_name lsoa_name, 
# MAGIC   e02.geography_code as msoa_code, e02.geography_name as msoa_name, 
# MAGIC   e0789.geography_code as authority_code, e0789.geography_name as authority_name,
# MAGIC   e0789.parent_geography_code as authority_parent_geography
# MAGIC   FROM curren_chd_geo_listings e01
# MAGIC   LEFT JOIN curren_chd_geo_listings e02 on e02.geography_code = e01.parent_geography_code
# MAGIC   LEFT JOIN curren_chd_geo_listings e0789 on e0789.geography_code = e02.parent_geography_code
# MAGIC   WHERE e01.geography_code like 'E01%' and e02.geography_code like 'E02%'
# MAGIC ),
# MAGIC auth_county as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC          msoa_code, msoa_name,
# MAGIC          authority_code, authority_name,
# MAGIC          e10.geography_code as county_code, e10.geography_name as county_name,
# MAGIC          e10.parent_geography_code as parent_geography
# MAGIC   FROM 
# MAGIC   lsoa_auth
# MAGIC     LEFT JOIN dss_corporate.ons_chd_geo_listings e10 on e10.geography_code = lsoa_auth.authority_parent_geography
# MAGIC   
# MAGIC   WHERE LEFT(authority_parent_geography,3) = 'E10'
# MAGIC ),
# MAGIC auth_met_county as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            NULL as county_code, NULL as county_name,           
# MAGIC            lsoa_auth.authority_parent_geography as region_code
# MAGIC   FROM lsoa_auth
# MAGIC   WHERE LEFT(authority_parent_geography,3) = 'E12'
# MAGIC ),
# MAGIC lsoa_region_code as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            county_code, county_name, auth_county.parent_geography as region_code
# MAGIC   FROM auth_county
# MAGIC   UNION ALL
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            county_code, county_name, region_code 
# MAGIC   FROM auth_met_county
# MAGIC ),
# MAGIC lsoa_region as (
# MAGIC   SELECT lsoa_code, lsoa_name,
# MAGIC            msoa_code, msoa_name,
# MAGIC            authority_code, authority_name,
# MAGIC            county_code, county_name, region_code, e12.geography_name as region_name FROM lsoa_region_code
# MAGIC   LEFT JOIN dss_corporate.ons_chd_geo_listings e12 on lsoa_region_code.region_code = e12.geography_code
# MAGIC )
# MAGIC SELECT * FROM lsoa_region

# COMMAND ----------

drop_table('ccu002_03_lsoa_region_lookup')
create_table('ccu002_03_lsoa_region_lookup')

# COMMAND ----------

# MAGIC %md ## COVID19 tractory table (run on 3 August 2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_ccu013_covid_trajectory AS
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory

# COMMAND ----------

drop_table('ccu002_03_ccu013_covid_trajectory')
create_table('ccu002_03_ccu013_covid_trajectory')
