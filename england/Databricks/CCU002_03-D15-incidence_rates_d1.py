# Databricks notebook source
# MAGIC %md # CCU002_03-D15-incidence_rates_d1
# MAGIC  
# MAGIC **Description** Generates Table 2
# MAGIC 
# MAGIC **Author(s)** Samantha Ip

# COMMAND ----------

# dbutils.widgets.text("event", "vac_str", "event_type_str")
# event = dbutils.widgets.get("event")
# vac_str = dbutils.widgets.get("vac_str")
# event_type_str = dbutils.widgets.get("event_type_str")

event = "any_myo_or_pericarditis"
vac_str = "vac_az"
event_type_str = "diag1"

print(f"event: {event}; vac_str: {vac_str}; event_type_str: {event_type_str}")


# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
from pyspark.sql.functions import col, count, isnan, lit, sum
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType

# COMMAND ----------

def examine_rows_column_value(df, colname, value):
    if value is None:
        tmp_df = df.where(col(colname).isNull())
    else:
        tmp_df = df[df[colname] == value]
    display(tmp_df)

    
def count_unique_pats(df, id_colname):
    n_unique_pats = df.agg(countDistinct(id_colname)).toPandas()
    return int(n_unique_pats.values)

def create_table(df, table_name:str, database_name:str="dars_nic_391419_j3w9t_collab", select_sql_script:str=None) -> None:
#   adapted from sam h 's save function
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifieda database."""
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.{table_name}""")
  df.createOrReplaceGlobalTempView(table_name)
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}""")
  spark.sql(f"""
                ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}
             """)
  

def count_not_null(c, nan_as_null=False):
#   https://stackoverflow.com/questions/33900726/count-number-of-non-nan-entries-in-each-column-of-spark-dataframe-with-pyspark
    pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True))
    return sum(pred.cast("integer")).alias(c)  

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ctrl & data

# COMMAND ----------

cohort_start_date = to_date(lit("2020-12-08"))
expo = "VACCINE"
cohort_end_date = to_date(lit("2021-05-17"))
cohort_end_date_str = "2021-05-17"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### read in data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- outcomes
# MAGIC REFRESH TABLE dars_nic_391419_j3w9t_collab.ccu002_03_cohort

# COMMAND ----------

survival_data = spark.table("dars_nic_391419_j3w9t_collab.ccu002_03_cohort")
display(survival_data)

# COMMAND ----------

if notes =="ever_hf1":
  print("ever_hf1")
  survival_data = survival_data.filter(col("cov_ever_hf")==1)
elif notes =="ever_hf0":
  print("ever_hf0")
  survival_data = survival_data.filter(col("cov_ever_hf")==0)
elif notes =="ever_cm1":
  print("ever_cm1")
  survival_data = survival_data.filter(col("cov_ever_cardiomyopathy")==1)
elif notes =="ever_cm0":
  print("ever_cm0")
  survival_data = survival_data.filter(col("cov_ever_cardiomyopathy")==0)
elif notes =="ever_hf0cm0":
  print("ever_hf0cm0")
  survival_data = survival_data.filter((col("cov_ever_hf")==0) & (col("cov_ever_cardiomyopathy")==0))
elif notes =="ever_hf0cm1":
  print("ever_hf0cm1")
  survival_data = survival_data.filter((col("cov_ever_hf")==0) & (col("cov_ever_cardiomyopathy")==1))
elif notes =="ever_hf1cm0":
  print("ever_hf1cm0")
  survival_data = survival_data.filter((col("cov_ever_hf")==1) & (col("cov_ever_cardiomyopathy")==0))
elif notes =="ever_hf1cm1":
  print("ever_hf1cm1")
  survival_data = survival_data.filter((col("cov_ever_hf")==1) & (col("cov_ever_cardiomyopathy")==1))

# COMMAND ----------

display(survival_data)

# COMMAND ----------

survival_data.columns

# COMMAND ----------

# [s for s in survival_data.columns if "THROMBO" in s]

# COMMAND ----------

def renameCols(df, old_columns, new_columns):
    for old_col,new_col in zip(old_columns,new_columns):
        df = df.withColumnRenamed(old_col,new_col)
    return df
# old_thrombo_cols = ["THROMBO_plus_ART_date","THROMBO_plus_VEN_date", "THROMBO_plus_other_art_date", "THROMBO_plus_DVT_ICVT_date", "THROMBO_plus_DVT_date", "THROMBO_plus_portal_date", "THROMBO_plus_MESENT_date"]
# new_thrombo_cols = ["THROMBO_plus_Arterial_event_date", "THROMBO_plus_Venous_event_date", "THROMBO_plus_other_arterial_embolism_date", "THROMBO_plus_ICVT_summ_event_date", "THROMBO_plus_DVT_summ_event_date", "THROMBO_plus_portal_vein_thrombosis_date", "THROMBO_plus_mesenteric_thrombus_date"]

# COMMAND ----------

# if event_type_str == "diag1":
#   event_name_in_table = "diag1_" + event + "_date"
# elif event_type_str == "anydiag":
#   event_name_in_table = "anydiag_" + event + "_date"
# elif event_type_str == "THROMBO_plus":
#   survival_data = renameCols(survival_data, old_thrombo_cols, new_thrombo_cols)
#   event_name_in_table = "THROMBO_plus_" + event + "_date"
# elif event_type_str == "death28days":
#   survival_data = survival_data.withColumn(f"death28days_{event}", when(col(f"death28days_{event}") == 1, col(f"diag1_{event}_date")).otherwise(None))
#   event_name_in_table = "death28days_" + event 
# display(survival_data)

event_name_in_table = "out_" + event 

event_col = [s for s in survival_data.columns if event_name_in_table in s]
# cols = ["NHS_NUMBER_DEID", "cov_sex", "death_date", "cov_age",  "exp_confirmed_covid19_date",  "exp_confirmed_covid_phenotype", "cov_region"] + event_col
# to match infection data's names to vaccination data's names (hack -- someone can rename if have time)
survival_data = renameCols(survival_data, ["cov_sex", "death_date", "cov_age",  "exp_confirmed_covid19_date",  "exp_confirmed_covid_phenotype", "cov_region"], 
                           ["SEX", "death_date", "AGE_AT_COHORT_START", "INFECTION_DATE_FIRST", "INFECTION_TYPE_FIRST"])
cols = ["NHS_NUMBER_DEID", "SEX", "death_date", "AGE_AT_COHORT_START", "INFECTION_DATE_FIRST", "INFECTION_TYPE_FIRST"] + event_col

cols = np.unique(cols).tolist() #in case of "death" as event
if event == "death":
  survival_data = survival_data.select(cols).withColumnRenamed('INFECTION_DATE_FIRST', 'expo_date').withColumn('DATE_OF_DEATH', col('death_date')).withColumnRenamed('INFECTION_TYPE_FIRST', 'INFECTION_TYPE').withColumnRenamed("death_date", 'event_date').withColumn("name", lit(event))
else:
  survival_data = survival_data.select(cols).withColumnRenamed('INFECTION_DATE_FIRST', 'expo_date').withColumn('DATE_OF_DEATH', col('death_date')).withColumnRenamed('INFECTION_TYPE_FIRST', 'INFECTION_TYPE').withColumnRenamed(event_name_in_table, 'event_date').withColumn("name", lit(event))


# COMMAND ----------

display(survival_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### minor wrangling
# MAGIC - vac: filter to keep only VACCINE_DOSE == "first dose"; restrict to only "NHS_NUMBER_DEID", "INFECTION_DATE" -- 'INFECTION_DATE' renamed as 'expo_date'
# MAGIC - cohort: identify IDs with expo_date < cohort_start_date -- exclude them
# MAGIC - outcomes: restrict to only "NHS_NUMBER_DEID", "record_date", "name" -- 'record_date' renamed as 'event_date'; filter to keep only event of interest
# MAGIC - survival_data: joined vac, cohort, outcomes

# COMMAND ----------

# MAGIC %md
# MAGIC ## find cohort_end_date
# MAGIC 
# MAGIC restrict all datasets to cohort_end_date

# COMMAND ----------

# max_expo_date, max_record_date, max_DATE_OF_DEATH = survival_data.select(max("expo_date"),  max("event_date"), max("DATE_OF_DEATH")).first()
# print(max_expo_date, max_record_date, max_DATE_OF_DEATH)

# COMMAND ----------

# [--- expo/event/DOD ---]

survival_data = survival_data.withColumn("DATE_OF_DEATH", \
              when( (survival_data["DATE_OF_DEATH"] < cohort_start_date) | (survival_data["DATE_OF_DEATH"] > cohort_end_date), None).otherwise(survival_data["DATE_OF_DEATH"]))
survival_data = survival_data.withColumn("event_date", \
              when((survival_data["event_date"] < cohort_start_date) | (survival_data["event_date"] > cohort_end_date), None).otherwise(survival_data["event_date"]))
survival_data = survival_data.withColumn("expo_date", \
              when((survival_data["expo_date"] < cohort_start_date) | (survival_data["expo_date"] > cohort_end_date), None).otherwise(survival_data["expo_date"]))


# COMMAND ----------

# MAGIC %md
# MAGIC ## END_DATE

# COMMAND ----------

# MAGIC %md
# MAGIC #### censoring at other expos

# COMMAND ----------

if covidpheno_str in ["non_hospitalised", "hospitalised"]:
  ls_vac = [covidpheno_str]
elif covidpheno_str == "all":
  ls_vac = ["non_hospitalised", "hospitalised"]
print(ls_vac)

# COMMAND ----------

# define DATE_VAC_CENSOR as expo_date of other vaccines, set expo/event_dates to None if on DVC or if after DoD
survival_data = survival_data\
  .withColumn("DATE_VAC_CENSOR", when(~col('INFECTION_TYPE').isin(ls_vac), f.col('expo_date')))\
  .withColumn("expo_date", when( (col("expo_date") > col("DATE_OF_DEATH")) | (col("expo_date") >= col("DATE_VAC_CENSOR")) , None).otherwise(col("expo_date")))\
  .withColumn("event_date", when((col("event_date") > col("DATE_OF_DEATH")) | (col("event_date") >= col("DATE_VAC_CENSOR")), None).otherwise(col("event_date")))\
  .withColumn("name", when(col("event_date").isNull(), None).otherwise(col("name")))
display(survival_data)

# COMMAND ----------

survival_data = survival_data\
  .withColumn('END_DATE', f.least(f.col('DATE_OF_DEATH'), f.col('event_date'), f.col("DATE_VAC_CENSOR"))).fillna({'END_DATE':cohort_end_date_str})\
  .withColumn('END_DATE', to_date(unix_timestamp(col('END_DATE'), 'yyyy-MM-dd').cast("timestamp")))
display(survival_data)

# COMMAND ----------

# set expo/event_dates > END_DATE to None

survival_data = survival_data.withColumn("event_date", \
              when(survival_data["event_date"] > col("END_DATE"), None).otherwise(survival_data["event_date"]))
survival_data = survival_data.withColumn("expo_date", \
              when(survival_data["expo_date"] > col("END_DATE"), None).otherwise(survival_data["expo_date"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## num events in periods

# COMMAND ----------

# MAGIC %md 
# MAGIC ### unexposed days

# COMMAND ----------

survival_data = survival_data.withColumn("num_unexposed_days",
                                         when(col("expo_date").isNotNull(), datediff(col("expo_date"), cohort_start_date))  # start[---)expo 
                                         .when(col("expo_date").isNull(), datediff(col("END_DATE"), cohort_start_date))  # start[---)non-expo censor date := min(DOD, DVC, event, cohort_end_date)
                                        )
survival_data = survival_data.withColumn("num_unexposed_days",
                                         when((col("expo_date").isNull()) & ((col("END_DATE") != col("DATE_VAC_CENSOR")) | (col("DATE_VAC_CENSOR").isNull())),
                                              col("num_unexposed_days")+1).otherwise(col("num_unexposed_days"))
                                        ) # start[---]if end date is not DVC, but any of DOD, event, cohort_end_date
survival_data = survival_data.withColumn("event_unexposed",
                                         when((col("event_date") < col("expo_date")) | (col("event_date").isNotNull() & col("expo_date").isNull()), 1)
                                        )
display(survival_data)       

# COMMAND ----------

survival_data.agg(*[count_not_null(c) for c in ["event_unexposed"]]).show()


# COMMAND ----------

# testing
# survival_data = survival_data.filter(col("AGE_AT_COHORT_START")<40)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 7 days postexpo

# COMMAND ----------

# should always have one post-expo day, events on expo day contribute to 28d postexpo
survival_data = survival_data.withColumn('7days_postexpo_date', f.date_add(survival_data['expo_date'], 6)) # expo[---)start of 28th day at 00:00

survival_data = survival_data.withColumn("end_date_7days_postexpo",
                                         when( col("7days_postexpo_date") < col("END_DATE") , col("7days_postexpo_date"))  # expo[---)28d end := min(28d, END_DATE)
                                         .when( col("7days_postexpo_date") >= col("END_DATE"),  col("END_DATE") )
                                    )

survival_data = survival_data.withColumn("num_days_in_wk1_1_postexpo", datediff(col("end_date_7days_postexpo"), col("expo_date"))) # start[---]if end date is not DVC, but any of DOD, event, cohort_end_date
                                         
survival_data = survival_data.withColumn("num_days_in_wk1_1_postexpo",
                                         when(
                                           ((col("end_date_7days_postexpo") != col("DATE_VAC_CENSOR")) |( col("DATE_VAC_CENSOR").isNull())) ,
                                           col("num_days_in_wk1_1_postexpo")+1).otherwise(col("num_days_in_wk1_1_postexpo")) # expo[---)28d end := min(28d, END_DATE)
                                    )


display(survival_data)

# COMMAND ----------

# expo[ --- 28d event ---]28d
survival_data = survival_data.withColumn("event_in_wk1_1_postexpo",
                                         when(
                                           (col("event_date") <= col("end_date_7days_postexpo")) & ( col("event_date") >= col("expo_date")  )
                                           , 1)
                                        )

display(survival_data)

# COMMAND ----------

# survival_data.select(max("event_in_7days_postexpo"),  max("expo_date"), max("num_days_in_7days_postexpo")).first()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### weeks: 2, 3-4, 5-8, 9-12, 13-26
# MAGIC ###### [-1- ... -7-) [-8- ... -14-)[-15- ... -28-)[-29- ... -56-)[-57- ... -84-)[-85- ... end)

# COMMAND ----------



# COMMAND ----------

middle_week_pairs = {'N_prevlastwk': [1, 2, 4, 8, 12], 'N_nowlastwk': [2, 4, 8, 12, 26]}
middle_week_pairs = pd.DataFrame(data=middle_week_pairs)
display(middle_week_pairs)

# COMMAND ----------

def middle_intervals(survival_data, N_prevlastwk, N_nowlastwk):
  survival_data = survival_data.withColumn('start_interval_date', f.date_add(survival_data['expo_date'], 7*N_prevlastwk)) 
  # survival_data = survival_data.withColumn('7days_postexpo_date', f.date_add(survival_data['expo_date'], 6)) # expo[---)start of 28th day at 00:00
  survival_data = survival_data.withColumn('open_end_interval_date', f.date_add(survival_data['start_interval_date'], 7*(N_nowlastwk-N_prevlastwk)-1)) # expo[---)start of 28th day at 00:00


  # survival_data = survival_data.withColumn("end_date_7days_postexpo",
  #                                          when( col("7days_postexpo_date") < col("END_DATE") , col("7days_postexpo_date"))  # expo[---)28d end := min(28d, END_DATE)
  #                                          .when( col("7days_postexpo_date") >= col("END_DATE"),  col("END_DATE") )
  #                                     )

  survival_data = survival_data.withColumn("open_end_interval_date_final",
                                           when( col("open_end_interval_date") < col("END_DATE") , col("open_end_interval_date"))  # expo[---)28d end := min(28d, END_DATE)
                                           .when( col("open_end_interval_date") >= col("END_DATE"),  col("END_DATE") )
                                      )

  survival_data = survival_data.withColumn(f"num_days_in_wk{N_prevlastwk+1}_{N_nowlastwk}_postexpo", 
                                           when( col("open_end_interval_date_final") >= col("start_interval_date"), 
                                           datediff(col("open_end_interval_date_final"), col("start_interval_date")))
                                          )# start[---]if end date is not DVC, but any of DOD, event, cohort_end_date

  survival_data = survival_data.withColumn(f"num_days_in_wk{N_prevlastwk+1}_{N_nowlastwk}_postexpo",
                                           when(
                                             ((col("open_end_interval_date_final") != col("DATE_VAC_CENSOR"))|( col("DATE_VAC_CENSOR").isNull())) ,
                                             col(f"num_days_in_wk{N_prevlastwk+1}_{N_nowlastwk}_postexpo")+1).otherwise(col(f"num_days_in_wk{N_prevlastwk+1}_{N_nowlastwk}_postexpo")) # expo[---)28d end := min(28d, END_DATE)
                                      )

  survival_data = survival_data.withColumn(f"event_in_wk{N_prevlastwk+1}_{N_nowlastwk}_postexpo",
                                           when(
                                             (col("event_date") <= col("open_end_interval_date_final")) & ( col("event_date") >= col("start_interval_date")  )
                                             , 1)
                                          )
  return survival_data

# COMMAND ----------

for index, row in middle_week_pairs.iterrows():
  
  survival_data = middle_intervals(survival_data, row['N_prevlastwk'].tolist(), row['N_nowlastwk'].tolist())

# COMMAND ----------

display(survival_data)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### > 26 weeks postexpo

# COMMAND ----------

survival_data = survival_data.withColumn('start_interval_date', f.date_add(survival_data['expo_date'], 7*26)) 
# survival_data = survival_data.withColumn('7days_postexpo_date', f.date_add(survival_data['expo_date'], 28)) 

survival_data = survival_data.withColumn("num_days_in_wk27_49_postexpo", 
                                         when( col("END_DATE") >= col("start_interval_date"), 
                                              datediff(col("END_DATE"),col("start_interval_date"))
                                             )
                                        )
survival_data = survival_data.withColumn("num_days_in_wk27_49_postexpo",
                                         when((col("END_DATE") != col("DATE_VAC_CENSOR"))| col("DATE_VAC_CENSOR").isNull(),
                                              col("num_days_in_wk27_49_postexpo")+1).otherwise(col("num_days_in_wk27_49_postexpo"))
                                        ) 


survival_data = survival_data.withColumn("event_in_wk27_49_postexpo",
                                         when(
                                           (col("event_date") >= col("start_interval_date")) 
                                           , 1)
                                        )
display(survival_data)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## cohort_agegp

# COMMAND ----------

display(survival_data)

# COMMAND ----------


cohort_agegp1_sexall = survival_data.filter((col("AGE_AT_COHORT_START") < 40))
cohort_agegp2_sexall = survival_data.filter((col("AGE_AT_COHORT_START") >= 40) & (col("AGE_AT_COHORT_START") <= 59))
cohort_agegp3_sexall = survival_data.filter((col("AGE_AT_COHORT_START") >= 60) & (col("AGE_AT_COHORT_START") <= 79))
cohort_agegp4_sexall = survival_data.filter((col("AGE_AT_COHORT_START") >= 80) )

# cohort_agegp1_sexall = survival_data.filter((col("AGE_AT_COHORT_START") >= 40) & (col("AGE_AT_COHORT_START") <= 59))

# cohort_agegp1_sexall = survival_data.filter((col("AGE_AT_COHORT_START") <= 40) & (col("AGE_AT_COHORT_START")>= 59))
# cohort_agegp1_sex1 = survival_data.filter((col("AGE_AT_COHORT_START") < 70) & (col("SEX") ==1))
# cohort_agegp1_sex2 = survival_data.filter((col("AGE_AT_COHORT_START") < 70) & (col("SEX") ==2))

# cohort_agegp2_sexall = survival_data.filter((col("AGE_AT_COHORT_START") >= 70))
# cohort_agegp2_sex1 = survival_data.filter((col("AGE_AT_COHORT_START") >= 70) & (col("SEX") ==1))
# cohort_agegp2_sex2 = survival_data.filter((col("AGE_AT_COHORT_START") >= 70) & (col("SEX") ==2))

display(cohort_agegp1_sexall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## final tables
# MAGIC - event counts, number of days in period of interest, incidence rates
# MAGIC - incidence rate: personyear per million people

# COMMAND ----------

from pyspark.sql.types import LongType, DoubleType

# COMMAND ----------

# def get_inci_rate(df_agegp_sex, colname_event_flag, colname_num_days_in_period):
#   events_count = (df_agegp_sex.agg(f.sum(colname_event_flag)).collect()[0][0])
#   if events_count == None:
#     events_count = 0.0
#     ir = 0.0
#     num_days_in_period = df_agegp_sex.agg(f.sum(colname_num_days_in_period)).collect()[0][0]/365.25
#     print(events_count, num_days_in_period, ir)
#     return events_count, num_days_in_period, ir
#   num_days_in_period = df_agegp_sex.agg(f.sum(colname_num_days_in_period)).collect()[0][0]/365.25
#   ir = (events_count*1e5  / num_days_in_period)
#   print(events_count, num_days_in_period, ir)
#   return events_count, num_days_in_period, ir

# def mk_df_agegp_period(period, event_period, num_period_days, cohort_agegp1_sexall, cohort_agegp1_sex1, cohort_agegp1_sex2):
#   events_sexall, n_days_sexall, ir_sexall = get_inci_rate(cohort_agegp1_sexall, event_period, num_period_days)
#   events_sex1, n_days_sex1, ir_sex1 = get_inci_rate(cohort_agegp1_sex1, event_period, num_period_days)
#   events_sex2, n_days_sex2, ir_sex2 = get_inci_rate(cohort_agegp1_sex2, event_period, num_period_days)
#   df = spark.createDataFrame(
#     [(
#         period,
#         ir_sexall,
#         ir_sex1,
#         ir_sex2,
#         events_sexall,
#         events_sex1,
#         events_sex2,
#         n_days_sexall,
#         n_days_sex1,
#         n_days_sex2
#       ), ],
#     ["period", "IR_py_sexall", "IR_py_sex1", "IR_py_sex2", "events_sexall","events_sex1", "events_sex2", "n_days_sexall", "n_days_sex1", "n_days_sex2"]  #column names
#   ) 
#   float_cols = ['IR_py_sexall', 'IR_py_sex1', "IR_py_sex2"]
#   int_cols = ["events_sexall","events_sex1", "events_sex2", "n_days_sexall", "n_days_sex1", "n_days_sex2"]
#   for col_name in float_cols:
#     df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
#   for col_name in int_cols:
#     df = df.withColumn(col_name, col(col_name).cast(LongType()))

#   return df
  
# def concat_df_agepg(agegp, event, covidpheno_str, cohort_agegp1_sexall, cohort_agegp1_sex1, cohort_agegp1_sex2):
#   if agegp =="agegp1":
#     agegp = "<70"
#   elif agegp =="agegp2":
#     agegp = ">=70"
#   elif agegp =="agegpall":
#     agegp = "all"
#   df1 = mk_df_agegp_period("unexposed", "event_unexposed", "num_unexposed_days", cohort_agegp1_sexall, cohort_agegp1_sex1, cohort_agegp1_sex2)
#   df2 = mk_df_agegp_period("28 days post-expo", "event_in_7days_postexpo", "num_days_in_7days_postexpo", cohort_agegp1_sexall, cohort_agegp1_sex1, cohort_agegp1_sex2)
#   df3 = mk_df_agegp_period(">28 days post-expo", "event_gt28days_postexpo", "num_days_gt28days_postexpo", cohort_agegp1_sexall, cohort_agegp1_sex1, cohort_agegp1_sex2)
#   df = df1.union(df2).union(df3)
#   df = df.withColumn("outcome", lit(event)).withColumn("agegroup", lit(agegp)).withColumn("vac", lit(covidpheno_str))
#   return df

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

def get_inci_rate(df_agegp_sex, colname_event_flag, colname_num_days_in_period):
  events_count = (df_agegp_sex.agg(f.sum(colname_event_flag)).collect()[0][0])
  if events_count == None:
    events_count = 0.0
    ir = 0.0
    num_days_in_period = df_agegp_sex.agg(f.sum(colname_num_days_in_period)).collect()[0][0]/365.25
    print(events_count, num_days_in_period, ir)
    return events_count, num_days_in_period, ir
  num_days_in_period = df_agegp_sex.agg(f.sum(colname_num_days_in_period)).collect()[0][0]/365.25
  ir = (events_count*1e5  / num_days_in_period)
  print(events_count, num_days_in_period, ir)
  return events_count, num_days_in_period, ir

def mk_df_agegp_period(period, event_period, num_period_days, cohort_agegp1_sexall):
  events_sexall, n_days_sexall, ir_sexall = get_inci_rate(cohort_agegp1_sexall, event_period, num_period_days)
  df = spark.createDataFrame(
    [(
        period,
        ir_sexall,
        events_sexall,
        n_days_sexall
      ), ],
    ["period", "IR_py_sexall", "events_sexall", "n_days_sexall"]  #column names
  ) 
  float_cols = ['IR_py_sexall']
  int_cols = ["events_sexall", "n_days_sexall"]
  for col_name in float_cols:
    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
  for col_name in int_cols:
    df = df.withColumn(col_name, col(col_name).cast(LongType()))

  return df
  
def concat_df_agepg(agegp, event, covidpheno_str, cohort_agegp1_sexall):
  if agegp =="agegp1":
    agegp = "<40"
  elif agegp =="agegp2":
    agegp = "40-59"
  elif agegp =="agegp3":
    agegp = "60-79"
  elif agegp =="agegp4":
    agegp = ">=80"
  elif agegp == "agegpall":
    agegp = "all"
#  2, 3-4, 5-8, 9_12, 13_26 num_days_in_wk27_49_postexpo
  df0 = mk_df_agegp_period("unexposed", "event_unexposed", "num_unexposed_days", cohort_agegp1_sexall)
  df1 = mk_df_agegp_period("week1", "event_in_wk1_1_postexpo", "num_days_in_wk1_1_postexpo", cohort_agegp1_sexall)
  df2 = mk_df_agegp_period("week2", "event_in_wk2_2_postexpo", "num_days_in_wk2_2_postexpo", cohort_agegp1_sexall)
  df3 = mk_df_agegp_period("week3_4", "event_in_wk3_4_postexpo", "num_days_in_wk3_4_postexpo", cohort_agegp1_sexall)
  df4 = mk_df_agegp_period("week5_8", "event_in_wk5_8_postexpo", "num_days_in_wk5_8_postexpo", cohort_agegp1_sexall)
  df5 = mk_df_agegp_period("week9_12", "event_in_wk9_12_postexpo", "num_days_in_wk9_12_postexpo", cohort_agegp1_sexall)
  df6 = mk_df_agegp_period("week13_26", "event_in_wk13_26_postexpo", "num_days_in_wk13_26_postexpo", cohort_agegp1_sexall)
  df7 = mk_df_agegp_period("week27_49", "event_in_wk27_49_postexpo", "num_days_in_wk27_49_postexpo", cohort_agegp1_sexall)
  dfs = [df0, df1,df2,df3,df4,df5,df6,df7]
  df = reduce(DataFrame.unionAll, dfs)
  df = df.withColumn("outcome", lit(event)).withColumn("agegroup", lit(agegp)).withColumn("vac", lit(covidpheno_str))
  return df

# COMMAND ----------

display(cohort_agegp4_sexall.filter(col("num_days_in_wk13_26_postexpo").isNotNull()))

# COMMAND ----------

concat_df_agepg("agegp4", event, covidpheno_str, cohort_agegp4_sexall)


# COMMAND ----------

df_final_agegp1 = concat_df_agepg("agegp1", event, covidpheno_str, cohort_agegp1_sexall)
df_final_agegp2 = concat_df_agepg("agegp2", event, covidpheno_str, cohort_agegp2_sexall)
df_final_agegp3 = concat_df_agepg("agegp3", event, covidpheno_str, cohort_agegp3_sexall)
df_final_agegp4 = concat_df_agepg("agegp4", event, covidpheno_str, cohort_agegp4_sexall)

display(df_final_agegp1)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,addition: any age
cohort_agegpall_sexall = survival_data
# cohort_agegpall_sex1 = survival_data.filter(col("SEX") ==1)
# cohort_agegpall_sex2 = survival_data.filter(col("SEX") ==2)
df_final_agegpall = concat_df_agepg("agegpall", event, covidpheno_str, cohort_agegpall_sexall)
display(df_final_agegpall)


# COMMAND ----------

df_final = df_final_agegp1.union(df_final_agegp2).union(df_final_agegp3).union(df_final_agegp4).union(df_final_agegpall)
display(df_final)

# COMMAND ----------

# fname = f"ccu002_01_ircounts_{event_type_str}_{event}_{covidpheno_str}_0721"
fname = f"ccu002_01_ircounts_{event}_{covidpheno_str}_{notes}_0827"
fpath = f"dars_nic_391419_j3w9t_collab.{fname}"
fname

# COMMAND ----------

create_table(df_final, table_name=fname, database_name="dars_nic_391419_j3w9t_collab", select_sql_script=None)
