# Databricks notebook source
# MAGIC %md # CCU002_03-D12-event_counts
# MAGIC 
# MAGIC **Description** This notebook counts the events for each analysis.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN cov_sex=1 THEN 'Men'
# MAGIC             WHEN cov_sex=2 THEN 'Women'
# MAGIC             ELSE '' END AS sex,
# MAGIC        CASE WHEN cov_age>=18 AND cov_age<40 THEN '<40'
# MAGIC             WHEN cov_age>=40 AND cov_age<60 THEN '40-59'
# MAGIC             WHEN cov_age>=60 AND cov_age<80 THEN '60-79'
# MAGIC             WHEN cov_age>=80 THEN '80+' 
# MAGIC             ELSE '' END AS age_grp, 
# MAGIC        SUM(CASE WHEN out_first_myocarditis IS NOT NULL AND ((vaccination_dose2_date>out_first_myocarditis) OR (vaccination_dose2_date IS NULL)) THEN 1 ELSE 0 END) AS first_myocarditis_before,
# MAGIC        SUM(CASE WHEN out_first_myocarditis IS NOT NULL AND vaccination_dose2_date<=out_first_myocarditis THEN 1 ELSE 0 END) AS first_myocarditis_after,
# MAGIC        SUM(CASE WHEN out_first_pericarditis IS NOT NULL AND ((vaccination_dose2_date>out_first_pericarditis) OR (vaccination_dose2_date IS NULL)) THEN 1 ELSE 0 END) AS first_pericarditis_before,
# MAGIC        SUM(CASE WHEN out_first_pericarditis IS NOT NULL AND vaccination_dose2_date<=out_first_pericarditis THEN 1 ELSE 0 END) AS first_pericarditis_after,
# MAGIC        SUM(CASE WHEN out_first_myo_or_pericarditis IS NOT NULL AND ((vaccination_dose2_date>out_first_myo_or_pericarditis) OR (vaccination_dose2_date IS NULL)) THEN 1 ELSE 0 END) AS first_myopericarditis_before,
# MAGIC        SUM(CASE WHEN out_first_myo_or_pericarditis IS NOT NULL AND vaccination_dose2_date<=out_first_myo_or_pericarditis THEN 1 ELSE 0 END) AS first_myopericarditis_after,
# MAGIC        SUM(CASE WHEN out_any_myocarditis IS NOT NULL AND ((vaccination_dose2_date>out_any_myocarditis) OR (vaccination_dose2_date IS NULL)) THEN 1 ELSE 0 END) AS any_myocarditis_before,
# MAGIC        SUM(CASE WHEN out_any_myocarditis IS NOT NULL AND vaccination_dose2_date<=out_any_myocarditis THEN 1 ELSE 0 END) AS any_myocarditis_after,
# MAGIC        SUM(CASE WHEN out_any_pericarditis IS NOT NULL AND ((vaccination_dose2_date>out_any_pericarditis) OR (vaccination_dose2_date IS NULL)) THEN 1 ELSE 0 END) AS any_pericarditis_before,
# MAGIC        SUM(CASE WHEN out_any_pericarditis IS NOT NULL AND vaccination_dose2_date<=out_any_pericarditis THEN 1 ELSE 0 END) AS any_pericarditis_after,
# MAGIC        SUM(CASE WHEN out_any_myo_or_pericarditis IS NOT NULL AND ((vaccination_dose2_date>out_any_myo_or_pericarditis) OR (vaccination_dose2_date IS NULL)) THEN 1 ELSE 0 END) AS any_myopericarditis_before,
# MAGIC        SUM(CASE WHEN out_any_myo_or_pericarditis IS NOT NULL AND vaccination_dose2_date<=out_any_myo_or_pericarditis THEN 1 ELSE 0 END) AS any_myopericarditis_after
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort
# MAGIC GROUP BY cov_sex, age_grp, vaccination_dose1_product
