# Databricks notebook source
# MAGIC %md # CCU002_03-D05-codelists
# MAGIC 
# MAGIC **Description** This notebook creates global temporary views for the drug codelists needed for the CCU002_01 infection project.
# MAGIC 
# MAGIC **Authors** Rochelle Knight, Venexia Walker

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

# MAGIC %md ## Drugs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE GLOBAL TEMPORARY VIEW ccu002_03_codelist_drugs AS
# MAGIC SELECT DISTINCT PrescribedBNFCode AS code, PrescribedBNFName AS term, 'BNF' AS system, 'antiplatelet_drugs' AS codelist
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE left(PrescribedBNFCode, 4) = '0209'
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT PrescribedBNFCode AS code, PrescribedBNFName AS term, 'BNF' AS system, 'bp_lowering_drugs' AS codelist
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE left(PrescribedBNFCode, 9) = '0205053A0' -- aliskiren
# MAGIC    OR left(PrescribedBNFCode, 6) = '020504' -- alpha blockers
# MAGIC    OR (left(PrescribedBNFCode, 4) = '0204' -- beta blockers
# MAGIC       AND NOT (left(PrescribedBNFCode, 9) = '0204000R0' -- exclude propranolol
# MAGIC       OR left(PrescribedBNFCode, 9) = '0204000Q0')) -- exclude propranolol
# MAGIC    OR left(PrescribedBNFCode, 6) = '020602' -- calcium channel blockers
# MAGIC    OR (left(PrescribedBNFCode, 6) = '020502' -- centrally acting antihypertensives
# MAGIC       AND NOT (left(PrescribedBNFCode, 8) = '0205020G' -- guanfacine because it is only used for ADHD
# MAGIC       OR left(PrescribedBNFCode, 9) = '0205052AE')) -- drugs for heart failure, not for hypertension
# MAGIC    OR left(PrescribedBNFCode, 6) = '020203' -- potassium sparing diuretics
# MAGIC    OR left(PrescribedBNFCode, 6) = '020201' -- thiazide diuretics
# MAGIC    OR left(PrescribedBNFCode, 6) = '020501' -- vasodilator antihypertensives
# MAGIC    OR left(PrescribedBNFCode, 7) = '0205051' -- angiotensin-converting enzyme inhibitors
# MAGIC    OR left(PrescribedBNFCode, 7) = '0205052' -- angiotensin-II receptor antagonists
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT PrescribedBNFCode AS code, PrescribedBNFName AS term, 'BNF' AS system, 'lipid_lowering_drugs' AS codelist
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE left(PrescribedBNFCode, 4) = '0212'
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT PrescribedBNFCode AS code, PrescribedBNFName AS term, 'BNF' AS system, 'anticoagulant_drugs' AS codelist
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE (left(PrescribedBNFCode, 6) = '020802'
# MAGIC        AND NOT (left(PrescribedBNFCode, 8) = '0208020I' OR left(PrescribedBNFCode, 8) = '0208020W'))
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT PrescribedBNFCode AS code, PrescribedBNFName AS term, 'BNF' AS system, 'cocp_drugs' AS codelist
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE left(PrescribedBNFCode, 6) = '070301'
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT PrescribedBNFCode AS code, PrescribedBNFName AS term, 'BNF' AS system, 'hrt_drugs' AS codelist
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE left(PrescribedBNFCode, 7) = '0604011'

# COMMAND ----------

# MAGIC %md ## Smoking

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE GLOBAL TEMPORARY VIEW ccu002_03_codelist_smoking AS
# MAGIC SELECT * 
# MAGIC FROM VALUES
# MAGIC ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","current_smoker","Light"),
# MAGIC ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","current_smoker","Heavy"),
# MAGIC ("160613002","Admitted tobacco consumption possibly untrue (finding)","current_smoker","Unknown"),
# MAGIC ("160619003","Rolls own cigarettes (finding)","current_smoker","Unknown"),
# MAGIC ("230056004","Cigarette consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("230057008","Cigar consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("230058003","Pipe tobacco consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("230060001","Light cigarette smoker (finding)","current_smoker","Light"),
# MAGIC ("230062009","Moderate cigarette smoker (finding)","current_smoker","Moderate"),
# MAGIC ("230065006","Chain smoker (finding)","current_smoker","Heavy"),
# MAGIC ("266918002","Tobacco smoking consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("446172000","Failed attempt to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("449868002","Smokes tobacco daily (finding)","current_smoker","Unknown"),
# MAGIC ("56578002","Moderate smoker (20 or less per day) (finding)","current_smoker","Moderate"),
# MAGIC ("56771006","Heavy smoker (over 20 per day) (finding)","current_smoker","Heavy"),
# MAGIC ("59978006","Cigar smoker (finding)","current_smoker","Unknown"),
# MAGIC ("65568007","Cigarette smoker (finding)","current_smoker","Unknown"),
# MAGIC ("134406006","Smoking reduced (finding)","current_smoker","Unknown"),
# MAGIC ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","current_smoker","Moderate"),
# MAGIC ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","current_smoker","Heavy"),
# MAGIC ("160612007","Keeps trying to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("160616005","Trying to give up smoking (finding)","current_smoker","Unknown"),
# MAGIC ("203191000000107","Wants to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("225934006","Smokes in bed (finding)","current_smoker","Unknown"),
# MAGIC ("230059006","Occasional cigarette smoker (finding)","current_smoker","Light"),
# MAGIC ("230063004","Heavy cigarette smoker (finding)","current_smoker","Heavy"),
# MAGIC ("230064005","Very heavy cigarette smoker (finding)","current_smoker","Heavy"),
# MAGIC ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","current_smoker","Light"),
# MAGIC ("266929003","Smoking started (finding)","current_smoker","Unknown"),
# MAGIC ("308438006","Smoking restarted (finding)","current_smoker","Unknown"),
# MAGIC ("394871007","Thinking about stopping smoking (finding)","current_smoker","Unknown"),
# MAGIC ("394872000","Ready to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("394873005","Not interested in stopping smoking (finding)","current_smoker","Unknown"),
# MAGIC ("401159003","Reason for restarting smoking (observable entity)","current_smoker","Unknown"),
# MAGIC ("413173009","Minutes from waking to first tobacco consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("428041000124106","Occasional tobacco smoker (finding)","current_smoker","Light"),
# MAGIC ("77176002","Smoker (finding)","current_smoker","Unknown"),
# MAGIC ("82302008","Pipe smoker (finding)","current_smoker","Unknown"),
# MAGIC ("836001000000109","Waterpipe tobacco consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","current_smoker","Light"),
# MAGIC ("160612007","Keeps trying to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("160613002","Admitted tobacco consumption possibly untrue (finding)","current_smoker","Unknown"),
# MAGIC ("160616005","Trying to give up smoking (finding)","current_smoker","Unknown"),
# MAGIC ("160619003","Rolls own cigarettes (finding)","current_smoker","Unknown"),
# MAGIC ("160625004","Date ceased smoking (observable entity)","current_smoker","Unknown"),
# MAGIC ("225934006","Smokes in bed (finding)","current_smoker","Unknown"),
# MAGIC ("230056004","Cigarette consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("230057008","Cigar consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("230059006","Occasional cigarette smoker (finding)","current_smoker","Light"),
# MAGIC ("230060001","Light cigarette smoker (finding)","current_smoker","Light"),
# MAGIC ("230062009","Moderate cigarette smoker (finding)","current_smoker","Moderate"),
# MAGIC ("230063004","Heavy cigarette smoker (finding)","current_smoker","Heavy"),
# MAGIC ("230064005","Very heavy cigarette smoker (finding)","current_smoker","Heavy"),
# MAGIC ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","current_smoker","Light"),
# MAGIC ("266929003","Smoking started (finding)","current_smoker","Unknown"),
# MAGIC ("394872000","Ready to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("401159003","Reason for restarting smoking (observable entity)","current_smoker","Unknown"),
# MAGIC ("449868002","Smokes tobacco daily (finding)","current_smoker","Unknown"),
# MAGIC ("65568007","Cigarette smoker (finding)","current_smoker","Unknown"),
# MAGIC ("134406006","Smoking reduced (finding)","current_smoker","Unknown"),
# MAGIC ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","current_smoker","Moderate"),
# MAGIC ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","current_smoker","Heavy"),
# MAGIC ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","current_smoker","Heavy"),
# MAGIC ("203191000000107","Wants to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("308438006","Smoking restarted (finding)","current_smoker","Unknown"),
# MAGIC ("394871007","Thinking about stopping smoking (finding)","current_smoker","Unknown"),
# MAGIC ("394873005","Not interested in stopping smoking (finding)","current_smoker","Unknown"),
# MAGIC ("401201003","Cigarette pack-years (observable entity)","current_smoker","Unknown"),
# MAGIC ("413173009","Minutes from waking to first tobacco consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("428041000124106","Occasional tobacco smoker (finding)","current_smoker","Light"),
# MAGIC ("446172000","Failed attempt to stop smoking (finding)","current_smoker","Unknown"),
# MAGIC ("56578002","Moderate smoker (20 or less per day) (finding)","current_smoker","Moderate"),
# MAGIC ("56771006","Heavy smoker (over 20 per day) (finding)","current_smoker","Heavy"),
# MAGIC ("59978006","Cigar smoker (finding)","current_smoker","Unknown"),
# MAGIC ("77176002","Smoker (finding)","current_smoker","Unknown"),
# MAGIC ("82302008","Pipe smoker (finding)","current_smoker","Unknown"),
# MAGIC ("836001000000109","Waterpipe tobacco consumption (observable entity)","current_smoker","Unknown"),
# MAGIC ("53896009","Tolerant ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","former_smoker","Unknown"),
# MAGIC ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","former_smoker","Unknown"),
# MAGIC ("160620009","Ex-pipe smoker (finding)","former_smoker","Unknown"),
# MAGIC ("160621008","Ex-cigar smoker (finding)","former_smoker","Unknown"),
# MAGIC ("228486009","Time since stopped smoking (observable entity)","former_smoker","Unknown"),
# MAGIC ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266928006","Ex-cigarette smoker amount unknown (finding)","former_smoker","Unknown"),
# MAGIC ("281018007","Ex-cigarette smoker (finding)","former_smoker","Unknown"),
# MAGIC ("735128000","Ex-smoker for less than 1 year (finding)","former_smoker","Unknown"),
# MAGIC ("8517006","Ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("1092031000000100","Ex-smoker amount unknown (finding)","former_smoker","Unknown"),
# MAGIC ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","former_smoker","Unknown"),
# MAGIC ("1092111000000100","Ex-light smoker (1-9/day) (finding)","former_smoker","Unknown"),
# MAGIC ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","former_smoker","Unknown"),
# MAGIC ("160617001","Stopped smoking (finding)","former_smoker","Unknown"),
# MAGIC ("160625004","Date ceased smoking (observable entity)","former_smoker","Unknown"),
# MAGIC ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","former_smoker","Unknown"),
# MAGIC ("360890004","Intolerant ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("360900008","Aggressive ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("48031000119106","Ex-smoker for more than 1 year (finding)","former_smoker","Unknown"),
# MAGIC ("492191000000103","Ex roll-up cigarette smoker (finding)","former_smoker","Unknown"),
# MAGIC ("53896009","Tolerant ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("735112005","Date ceased using moist tobacco (observable entity)","former_smoker","Unknown"),
# MAGIC ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","former_smoker","Unknown"),
# MAGIC ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","former_smoker","Unknown"),
# MAGIC ("1092111000000100","Ex-light smoker (1-9/day) (finding)","former_smoker","Unknown"),
# MAGIC ("228486009","Time since stopped smoking (observable entity)","former_smoker","Unknown"),
# MAGIC ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266928006","Ex-cigarette smoker amount unknown (finding)","former_smoker","Unknown"),
# MAGIC ("360900008","Aggressive ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("492191000000103","Ex roll-up cigarette smoker (finding)","former_smoker","Unknown"),
# MAGIC ("735112005","Date ceased using moist tobacco (observable entity)","former_smoker","Unknown"),
# MAGIC ("735128000","Ex-smoker for less than 1 year (finding)","former_smoker","Unknown"),
# MAGIC ("1092031000000100","Ex-smoker amount unknown (finding)","former_smoker","Unknown"),
# MAGIC ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","former_smoker","Unknown"),
# MAGIC ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","former_smoker","Unknown"),
# MAGIC ("160617001","Stopped smoking (finding)","former_smoker","Unknown"),
# MAGIC ("160620009","Ex-pipe smoker (finding)","former_smoker","Unknown"),
# MAGIC ("160621008","Ex-cigar smoker (finding)","former_smoker","Unknown"),
# MAGIC ("230058003","Pipe tobacco consumption (observable entity)","former_smoker","Unknown"),
# MAGIC ("230065006","Chain smoker (finding)","former_smoker","Unknown"),
# MAGIC ("266918002","Tobacco smoking consumption (observable entity)","former_smoker","Unknown"),
# MAGIC ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","former_smoker","Unknown"),
# MAGIC ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","former_smoker","Unknown"),
# MAGIC ("281018007","Ex-cigarette smoker (finding)","former_smoker","Unknown"),
# MAGIC ("360890004","Intolerant ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("48031000119106","Ex-smoker for more than 1 year (finding)","former_smoker","Unknown"),
# MAGIC ("8517006","Ex-smoker (finding)","former_smoker","Unknown"),
# MAGIC ("221000119102","Never smoked any substance (finding)","never_smoker","NA"),
# MAGIC ("266919005","Never smoked tobacco (finding)","never_smoker","NA"),
# MAGIC ("221000119102","Never smoked any substance (finding)","never_smoker","NA"),
# MAGIC ("266919005","Never smoked tobacco (finding)","never_smoker","NA")
# MAGIC AS tab(conceptID, description, smoking_status, severity);

# COMMAND ----------

# MAGIC %md ## Pregnancy and birth

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW ccu002_03_codelist_pregnancy_birth AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("171057006","Pregnancy alcohol education (procedure)"),
# MAGIC ("72301000119103","Asthma in pregnancy (disorder)"),
# MAGIC ("10742121000119104","Asthma in mother complicating childbirth (disorder)"),
# MAGIC ("10745291000119103","Malignant neoplastic disease in mother complicating childbirth (disorder)"),
# MAGIC ("10749871000119100","Malignant neoplastic disease in pregnancy (disorder)"),
# MAGIC ("20753005","Hypertensive heart disease complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("237227006","Congenital heart disease in pregnancy (disorder)"),
# MAGIC ("169501005","Pregnant, diaphragm failure (finding)"),
# MAGIC ("169560008","Pregnant - urine test confirms (finding)"),
# MAGIC ("169561007","Pregnant - blood test confirms (finding)"),
# MAGIC ("169562000","Pregnant - vaginal examination confirms (finding)"),
# MAGIC ("169565003","Pregnant - planned (finding)"),
# MAGIC ("169566002","Pregnant - unplanned - wanted (finding)"),
# MAGIC ("413567003","Aplastic anemia associated with pregnancy (disorder)"),
# MAGIC ("91948008","Asymptomatic human immunodeficiency virus infection in pregnancy (disorder)"),
# MAGIC ("169488004","Contraceptive intrauterine device failure - pregnant (finding)"),
# MAGIC ("169508004","Pregnant, sheath failure (finding)"),
# MAGIC ("169564004","Pregnant - on abdominal palpation (finding)"),
# MAGIC ("77386006","Pregnant (finding)"),
# MAGIC ("10746341000119109","Acquired immune deficiency syndrome complicating childbirth (disorder)"),
# MAGIC ("10759351000119103","Sickle cell anemia in mother complicating childbirth (disorder)"),
# MAGIC ("10757401000119104","Pre-existing hypertensive heart and chronic kidney disease in mother complicating childbirth (disorder)"),
# MAGIC ("10757481000119107","Pre-existing hypertensive heart and chronic kidney disease in mother complicating pregnancy (disorder)"),
# MAGIC ("10757441000119102","Pre-existing hypertensive heart disease in mother complicating childbirth (disorder)"),
# MAGIC ("10759031000119106","Pre-existing hypertensive heart disease in mother complicating pregnancy (disorder)"),
# MAGIC ("1474004","Hypertensive heart AND renal disease complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("199006004","Pre-existing hypertensive heart disease complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("199007008","Pre-existing hypertensive heart and renal disease complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("22966008","Hypertensive heart AND renal disease complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("59733002","Hypertensive heart disease complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("171054004","Pregnancy diet education (procedure)"),
# MAGIC ("106281000119103","Pre-existing diabetes mellitus in mother complicating childbirth (disorder)"),
# MAGIC ("10754881000119104","Diabetes mellitus in mother complicating childbirth (disorder)"),
# MAGIC ("199225007","Diabetes mellitus during pregnancy - baby delivered (disorder)"),
# MAGIC ("237627000","Pregnancy and type 2 diabetes mellitus (disorder)"),
# MAGIC ("609563008","Pre-existing diabetes mellitus in pregnancy (disorder)"),
# MAGIC ("609566000","Pregnancy and type 1 diabetes mellitus (disorder)"),
# MAGIC ("609567009","Pre-existing type 2 diabetes mellitus in pregnancy (disorder)"),
# MAGIC ("199223000","Diabetes mellitus during pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("199227004","Diabetes mellitus during pregnancy - baby not yet delivered (disorder)"),
# MAGIC ("609564002","Pre-existing type 1 diabetes mellitus in pregnancy (disorder)"),
# MAGIC ("76751001","Diabetes mellitus in mother complicating pregnancy, childbirth AND/OR puerperium (disorder)"),
# MAGIC ("526961000000105","Pregnancy advice for patients with epilepsy (procedure)"),
# MAGIC ("527041000000108","Pregnancy advice for patients with epilepsy not indicated (situation)"),
# MAGIC ("527131000000100","Pregnancy advice for patients with epilepsy declined (situation)"),
# MAGIC ("10753491000119101","Gestational diabetes mellitus in childbirth (disorder)"),
# MAGIC ("40801000119106","Gestational diabetes mellitus complicating pregnancy (disorder)"),
# MAGIC ("10562009","Malignant hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("198944004","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - delivered (disorder)"),
# MAGIC ("198945003","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - delivered with postnatal complication (disorder)"),
# MAGIC ("198946002","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - not delivered (disorder)"),
# MAGIC ("198949009","Renal hypertension complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("198951008","Renal hypertension complicating pregnancy, childbirth and the puerperium - delivered (disorder)"),
# MAGIC ("198954000","Renal hypertension complicating pregnancy, childbirth and the puerperium with postnatal complication (disorder)"),
# MAGIC ("199005000","Pre-existing hypertension complicating pregnancy, childbirth and puerperium (disorder)"),
# MAGIC ("23717007","Benign essential hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("26078007","Hypertension secondary to renal disease complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("29259002","Malignant hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("65402008","Pre-existing hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("8218002","Chronic hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("10752641000119102","Eclampsia with pre-existing hypertension in childbirth (disorder)"),
# MAGIC ("118781000119108","Pre-existing hypertensive chronic kidney disease in mother complicating pregnancy (disorder)"),
# MAGIC ("18416000","Essential hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("198942000","Benign essential hypertension complicating pregnancy, childbirth and the puerperium (disorder)"),
# MAGIC ("198947006","Benign essential hypertension complicating pregnancy, childbirth and the puerperium with postnatal complication (disorder)"),
# MAGIC ("198952001","Renal hypertension complicating pregnancy, childbirth and the puerperium - delivered with postnatal complication (disorder)"),
# MAGIC ("198953006","Renal hypertension complicating pregnancy, childbirth and the puerperium - not delivered (disorder)"),
# MAGIC ("199008003","Pre-existing secondary hypertension complicating pregnancy, childbirth and puerperium (disorder)"),
# MAGIC ("34694006","Pre-existing hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("37618003","Chronic hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("48552006","Hypertension secondary to renal disease complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("71874008","Benign essential hypertension complicating AND/OR reason for care during childbirth (disorder)"),
# MAGIC ("78808002","Essential hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
# MAGIC ("91923005","Acquired immunodeficiency syndrome virus infection associated with pregnancy (disorder)"),
# MAGIC ("10755671000119100","Human immunodeficiency virus in mother complicating childbirth (disorder)"),
# MAGIC ("721166000","Human immunodeficiency virus complicating pregnancy childbirth and the puerperium (disorder)"),
# MAGIC ("449369001","Stopped smoking before pregnancy (finding)"),
# MAGIC ("449345000","Smoked before confirmation of pregnancy (finding)"),
# MAGIC ("449368009","Stopped smoking during pregnancy (finding)"),
# MAGIC ("88144003","Removal of ectopic interstitial uterine pregnancy requiring total hysterectomy (procedure)"),
# MAGIC ("240154002","Idiopathic osteoporosis in pregnancy (disorder)"),
# MAGIC ("956951000000104","Pertussis vaccination in pregnancy (procedure)"),
# MAGIC ("866641000000105","Pertussis vaccination in pregnancy declined (situation)"),
# MAGIC ("956971000000108","Pertussis vaccination in pregnancy given by other healthcare provider (finding)"),
# MAGIC ("169563005","Pregnant - on history (finding)"),
# MAGIC ("10231000132102","In-vitro fertilization pregnancy (finding)"),
# MAGIC ("134781000119106","High risk pregnancy due to recurrent miscarriage (finding)"),
# MAGIC ("16356006","Multiple pregnancy (disorder)"),
# MAGIC ("237239003","Low risk pregnancy (finding)"),
# MAGIC ("276367008","Wanted pregnancy (finding)"),
# MAGIC ("314204000","Early stage of pregnancy (finding)"),
# MAGIC ("439311009","Intends to continue pregnancy (finding)"),
# MAGIC ("713575004","Dizygotic twin pregnancy (disorder)"),
# MAGIC ("80997009","Quintuplet pregnancy (disorder)"),
# MAGIC ("1109951000000101","Pregnancy insufficiently advanced for reliable antenatal screening (finding)"),
# MAGIC ("1109971000000105","Pregnancy too advanced for reliable antenatal screening (finding)"),
# MAGIC ("237238006","Pregnancy with uncertain dates (finding)"),
# MAGIC ("444661007","High risk pregnancy due to history of preterm labor (finding)"),
# MAGIC ("459166009","Dichorionic diamniotic twin pregnancy (disorder)"),
# MAGIC ("459167000","Monochorionic twin pregnancy (disorder)"),
# MAGIC ("459168005","Monochorionic diamniotic twin pregnancy (disorder)"),
# MAGIC ("459171002","Monochorionic monoamniotic twin pregnancy (disorder)"),
# MAGIC ("47200007","High risk pregnancy (finding)"),
# MAGIC ("60810003","Quadruplet pregnancy (disorder)"),
# MAGIC ("64254006","Triplet pregnancy (disorder)"),
# MAGIC ("65147003","Twin pregnancy (disorder)"),
# MAGIC ("713576003","Monozygotic twin pregnancy (disorder)"),
# MAGIC ("171055003","Pregnancy smoking education (procedure)"),
# MAGIC ("10809101000119109","Hypothyroidism in childbirth (disorder)"),
# MAGIC ("428165003","Hypothyroidism in pregnancy (disorder)")
# MAGIC 
# MAGIC AS tab(code, term)

# COMMAND ----------

# MAGIC %md ## Prostate cancer

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW ccu002_03_codelist_prostate_cancer AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("126906006","Neoplasm of prostate"),
# MAGIC ("81232004","Radical cystoprostatectomy"),
# MAGIC ("176106009","Radical cystoprostatourethrectomy"),
# MAGIC ("176261008","Radical prostatectomy without pelvic node excision"),
# MAGIC ("176262001","Radical prostatectomy with pelvic node sampling"),
# MAGIC ("176263006","Radical prostatectomy with pelvic lymphadenectomy"),
# MAGIC ("369775001","Gleason Score 2-4: Well differentiated"),
# MAGIC ("369777009","Gleason Score 8-10: Poorly differentiated"),
# MAGIC ("385377005","Gleason grade finding for prostatic cancer (finding)"),
# MAGIC ("394932008","Gleason prostate grade 5-7 (medium) (finding)"),
# MAGIC ("399068003","Malignant tumor of prostate (disorder)"),
# MAGIC ("428262008","History of malignant neoplasm of prostate (situation)")
# MAGIC 
# MAGIC AS tab(code, term)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Depression

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_depression as 
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'depression'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obesity 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu002_03_codelist_BMI_obesity as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%BMI_obesity'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')
# MAGIC union all
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ('BMI_obesity','ICD10','E66','Diagnosis of obesity','','')
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPD

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu002_03_codelist_COPD as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%COPD%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hypertension

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu002_03_codelist_hypertension as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%hypertension%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10'
# MAGIC or terminology like 'DMD')
# MAGIC union all
# MAGIC select *
# MAGIC from values
# MAGIC ('hypertension','ICD10','I10','Essential (primary) hypertension','',''),
# MAGIC ('hypertension','ICD10','I11','Hypertensive heart disease','',''),
# MAGIC ('hypertension','ICD10','I12','Hypertensive renal disease','',''),
# MAGIC ('hypertension','ICD10','I13','Hypertensive heart and renal disease','',''),
# MAGIC ('hypertension','ICD10','I15','Secondary hypertension','','')
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diabetes

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu002_03_codelist_diabetes as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%diabetes%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10'
# MAGIC or terminology like 'DMD')
# MAGIC union all
# MAGIC select *
# MAGIC from values
# MAGIC ('diabetes','ICD10','E10','Insulin-dependent diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E11','Non-insulin-dependent diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E12','Malnutrition-related diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','O242','Diabetes mellitus in pregnancy: Pre-existing malnutrition-related diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E13','Other specified diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','E14','Unspecified diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','G590','Diabetic mononeuropathy','',''),
# MAGIC ('diabetes','ICD10','G632','Diabetic polyneuropathy','',''),
# MAGIC ('diabetes','ICD10','H280','Diabetic cataract','',''),
# MAGIC ('diabetes','ICD10','H360','Diabetic retinopathy','',''),
# MAGIC ('diabetes','ICD10','M142','Diabetic arthropathy','',''),
# MAGIC ('diabetes','ICD10','N083','Glomerular disorders in diabetes mellitus','',''),
# MAGIC ('diabetes','ICD10','O240','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent','',''),
# MAGIC ('diabetes','ICD10','O241','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent','',''),
# MAGIC ('diabetes','ICD10','O243','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified','','')
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cancer

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view ccu002_03_codelist_cancer as
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%cancer%'
# MAGIC and 
# MAGIC (terminology like 'SNOMED'
# MAGIC or terminology like 'ICD10')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liver disease

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_liver AS
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like '%liver%'
# MAGIC and 
# MAGIC terminology like 'ICD10'
# MAGIC union all
# MAGIC SELECT
# MAGIC 	pheno.name, 
# MAGIC     CONCAT(pheno.terminology, '_SNOMEDmapped') AS terminology,
# MAGIC 	pheno.term AS term, 
# MAGIC 	mapfile.SCT_CONCEPTID AS code,
# MAGIC     "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM
# MAGIC 	bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127 AS pheno,
# MAGIC 	dss_corporate.read_codes_map_ctv3_to_snomed AS mapfile 
# MAGIC WHERE pheno.name LIKE  "%liver%" 
# MAGIC AND pheno.terminology ="CTV3"
# MAGIC AND pheno.code = mapfile.CTV3_CONCEPTID 
# MAGIC AND mapfile.IS_ASSURED = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dementia

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_dementia AS
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'dementia%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CKD

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_ckd AS
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'CKD%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')

# COMMAND ----------

# MAGIC %md
# MAGIC ## AMI

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_ami AS
# MAGIC SELECT terminology, 
# MAGIC        code, 
# MAGIC        term, 
# MAGIC        code_type, 
# MAGIC        recorddate,
# MAGIC        'AMI' AS name
# MAGIC FROM bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC WHERE name like 'AMI%' 
# MAGIC AND (terminology like 'ICD10' or terminology like 'SNOMED')
# MAGIC AND (code!='I25.2') -- previously 'AMI_covariate_only'
# MAGIC AND (code!='I24.1') -- previously 'AMI_covariate_only'

# COMMAND ----------

# MAGIC %md
# MAGIC ## VT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu002_03_codelist_vt as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("DVT_DVT","ICD10","I80","Phlebitis and thrombophlebitis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.8","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.9","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.0","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.3","Other vein thrombosis","1","20210127"),
# MAGIC ("other_DVT","ICD10","I82.2","Other vein thrombosis","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O22.3","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O87.1","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O87.9","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("DVT_pregnancy","ICD10","O88.2","Thrombosis during pregnancy and puerperium","1","20210127"),
# MAGIC ("ICVT_pregnancy","ICD10","O22.5","ntracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
# MAGIC ("ICVT_pregnancy","ICD10","O87.3","ntracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
# MAGIC ("portal_vein_thrombosis","ICD10","I81","Portal vein thrombosis","1","20210127")
# MAGIC --("VT_covariate_only","ICD10","O08.2","Embolism following abortion and ectopic and molar pregnancy","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ## DVT_ICVT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu002_03_codelist_DVT_ICVT as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("DVT_ICVT","ICD10","G08","Intracranial venous thrombosis","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","I67.6","Intracranial venous thrombosis","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","I63.6","Intracranial venous thrombosis","1","20210127")
# MAGIC --("DVT_ICVT_covariate_only","SNOMED","195230003","Cerebral infarction due to cerebral venous thrombosis,  nonpyogenic","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ## PE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu002_03_codelist_PE as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC 
# MAGIC ("PE","ICD10","I26.0","Pulmonary embolism","1","20210127"),
# MAGIC ("PE","ICD10","I26.9","Pulmonary embolism","1","20210127")
# MAGIC --("PE_covariate_only","SNOMED","438773007","Pulmonary embolism with mention of acute cor pulmonale","1","20210127"),
# MAGIC --("PE_covariate_only","SNOMED","133971000119108","Pulmonary embolism with mention of acute cor pulmonale","1","20210127")
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ischemic stroke 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu002_03_codelist_stroke_IS as
# MAGIC 
# MAGIC with cte as
# MAGIC (select terminology, code, term, code_type, RecordDate,
# MAGIC case 
# MAGIC when terminology like 'SNOMED' then 'stroke_isch'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_IS'
# MAGIC AND terminology ='SNOMED'
# MAGIC AND code_type='1' 
# MAGIC AND RecordDate='20210127'
# MAGIC )
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("stroke_isch","ICD10","I63.0","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.1","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.2","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.3","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.4","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.5","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.8","Cerebral infarction","1","20210127"),
# MAGIC ("stroke_isch","ICD10","I63.9","Cerebral infarction","1","20210127")
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)
# MAGIC union all
# MAGIC select new_name as name, terminology, code, term, code_type, RecordDate
# MAGIC from cte

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stroke, NOS 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_stroke_NOS AS
# MAGIC 
# MAGIC select *,
# MAGIC case 
# MAGIC when name like 'stroke_NOS%' then 'stroke_isch'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_NOS%'
# MAGIC and 
# MAGIC (terminology like 'ICD10'
# MAGIC or terminology like 'SNOMED')
# MAGIC and term != 'Stroke in the puerperium (disorder)'
# MAGIC and term != 'Cerebrovascular accident (disorder)'
# MAGIC and term != 'Right sided cerebral hemisphere cerebrovascular accident (disorder)'
# MAGIC and term != 'Left sided cerebral hemisphere cerebrovascular accident (disorder)'
# MAGIC and term != 'Brainstem stroke syndrome (disorder)'
# MAGIC AND code_type='1' 
# MAGIC AND RecordDate='20210127'

# COMMAND ----------

# MAGIC %md
# MAGIC ## stroke_SAH

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_stroke_SAH AS
# MAGIC select terminology, code, term, code_type, recorddate,
# MAGIC case 
# MAGIC when terminology like 'ICD10' then 'stroke_SAH_HS'
# MAGIC end as new_name
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'stroke_SAH%'
# MAGIC and 
# MAGIC terminology like 'ICD10'

# COMMAND ----------

# MAGIC %md
# MAGIC ## stroke_HS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_stroke_HS AS
# MAGIC SELECT terminology, 
# MAGIC        code, 
# MAGIC        term, 
# MAGIC        code_type, 
# MAGIC        recorddate,
# MAGIC        'stroke_SAH_HS' AS name
# MAGIC FROM bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC WHERE name like 'stroke_HS%'
# MAGIC AND (terminology like 'ICD10' or terminology like 'SNOMED') -- SNOMED codes previously used for covariate only

# COMMAND ----------

# MAGIC %md
# MAGIC ## Thrombophilia

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu002_03_codelist_thrombophilia as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("thrombophilia","ICD10","D68.5","Primary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","ICD10","D68.6","Other thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439001009","Acquired thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441882000","History of thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439698008","Primary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","234467004","Thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441697004","Thrombophilia associated with pregnancy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442760001","Thrombophilia caused by antineoplastic agent therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442197003","Thrombophilia caused by drug therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442654007","Thrombophilia caused by hormone therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442363001","Thrombophilia caused by vascular device","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439126002","Thrombophilia due to acquired antithrombin III deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439002002","Thrombophilia due to acquired protein C deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439125003","Thrombophilia due to acquired protein S deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441079006","Thrombophilia due to antiphospholipid antibody","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441762006","Thrombophilia due to immobilisation","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442078001","Thrombophilia due to malignant neoplasm","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441946009","Thrombophilia due to myeloproliferative disorder","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441990004","Thrombophilia due to paroxysmal nocturnal haemoglobinuria","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441945008","Thrombophilia due to trauma","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442121006","Thrombophilia due to vascular anomaly","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439698008","Hereditary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","783250007","Hereditary thrombophilia due to congenital histidine-rich (poly-L) glycoprotein deficiency","1",	"20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Thrombocytopenia

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view ccu002_03_codelist_TCP as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("thrombocytopenia","ICD10","D69.3","Thrombocytopenia","1","20210127"),
# MAGIC ("thrombocytopenia","ICD10","D69.4","Thrombocytopenia","1","20210127"),
# MAGIC ("thrombocytopenia","ICD10","D69.5","Thrombocytopenia","1","20210127"),
# MAGIC ("thrombocytopenia","ICD10","D69.6","Thrombocytopenia","1","20210127"),
# MAGIC ("TTP","ICD10","M31.1","Thrombotic microangiopathy","1","20210127")
# MAGIC --("TCP_covariate_only","SNOMED","74576004","Acquired thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","439007008","Acquired thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","28505005","Acute idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","128091003","Autoimmune thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","13172003","Autoimmune thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","438476003","Autoimmune thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","111588002","Heparin associated thrombotic thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","73397007","Heparin induced thrombocytopaenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","438492008","Hereditary thrombocytopenic disorder","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","441511006","History of immune thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","49341000119108","History of thrombocytopaenia",	"1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","726769004","HIT (Heparin induced thrombocytopenia) antibody","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","371106008","Idiopathic maternal thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","32273002","Idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","2897005","Immune thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","32273002","Immune thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","36070007","Immunodeficiency with thrombocytopenia AND eczema","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","33183004","Post infectious thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","267534000","Primary thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","154826009","Secondary thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","866152006","Thrombocytopenia due to 2019 novel coronavirus","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","82190001","Thrombocytopenia due to defective platelet production","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","78345002","Thrombocytopenia due to diminished platelet production","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","191323001","Thrombocytopenia due to extracorporeal circulation of blood","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","87902006","Thrombocytopenia due to non-immune destruction","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","302873008","Thrombocytopenic purpura",	"1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","417626001","Thrombocytopenic purpura associated with metabolic disorder","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","402653004","Thrombocytopenic purpura due to defective platelet production","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","402654005","Thrombocytopenic purpura due to platelet consumption","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","78129009","Thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","441322009","Drug induced thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","19307009","Drug-induced immune thrombocytopenia","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","783251006","Hereditary thrombocytopenia with normal platelets","1","20210127"),
# MAGIC --("TCP_covariate_only","SNOMED","191322006","Thrombocytopenia caused by drugs","1","20210127")
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retinal Infarction

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_retinal_infarction as
# MAGIC select *
# MAGIC from values
# MAGIC ("stroke_isch","ICD10","H34","Retinal vascular occlusions","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other arterial embolism

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_arterial_embolism as
# MAGIC select *
# MAGIC from values
# MAGIC ("other_arterial_embolism","ICD10","I74","arterial embolism and thrombosis","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disseminated intravascular coagulation (DIC)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_DIC as
# MAGIC select *
# MAGIC from values
# MAGIC ("DIC","ICD10","D65","Disseminated intravascular coagulation","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mesenteric thrombus

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_mesenteric_thrombus as
# MAGIC select *
# MAGIC from values
# MAGIC ("mesenteric_thrombus","ICD10","K55.9","Acute vascular disorders of intestine","1","20210127"),
# MAGIC ("mesenteric_thrombus","ICD10","K55.0","Acute vascular disorders of intestine","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spinal stroke

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_spinal_stroke as
# MAGIC select *
# MAGIC from values
# MAGIC ("stroke_isch","ICD10","G95.1","Avascular myelopathies (arterial or venous)","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md ## Fracture

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_fracture as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("fracture","ICD10","S720","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S721","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S723","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S724","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S727","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S728","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S729","Upper leg fracture","",""),
# MAGIC ("fracture","ICD10","S820","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S821","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S822","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S823","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S824","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S825","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S826","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S827","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S828","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S829","Lower leg fracture","",""),
# MAGIC ("fracture","ICD10","S920","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S921","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S922","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S923","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S927","Foot fracture","",""),
# MAGIC ("fracture","ICD10","S929","Foot fracture","",""),
# MAGIC ("fracture","ICD10","T12","Fracture of lower limb","",""),
# MAGIC ("fracture","ICD10","T025","Fractures involving multiple regions of both lower limbs","",""),
# MAGIC ("fracture","ICD10","T023","Fractures involving multiple regions of both lower limbs","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arterial dissection

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_artery_dissect as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("artery_dissect","ICD10","I71.0","Dissection of aorta [any part]","",""),
# MAGIC ("artery_dissect","ICD10","I72.0","Aneurysm and dissection of carotid artery","",""),
# MAGIC ("artery_dissect","ICD10","I72.1","Aneurysm and dissection of artery of upper extremity","",""),
# MAGIC ("artery_dissect","ICD10","I72.6","Dissection of vertebral artery","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Life threatening arrhythmias

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_life_arrhythmias as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("life_arrhythmia","ICD10","I46.0","Cardiac arrest with successful resuscitation","",""),
# MAGIC ("life_arrhythmia","ICD10","I46.1 ","Sudden cardiac death, so described","",""),
# MAGIC ("life_arrhythmia","ICD10","I46.9","Cardiac arrest, unspecified","",""),
# MAGIC ("life_arrhythmia","ICD10","I47.0","Re-entry ventricular arrhythmia","",""),
# MAGIC ("life_arrhythmia","ICD10","I47.2","Ventricular tachycardia","",""),
# MAGIC ("life_arrhythmia","ICD10","I49.0","Ventricular fibrillation and flutter","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cardiomyopathy

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_cardiomyopathy as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("cardiomyopathy","ICD10","I42.0","Dilated cardiomyopathy","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.3","Endomyocardial (eosinophilic) disease","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.5","Other restrictive cardiomyopathy","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.7","Cardiomyopathy due to drugs and other external agents","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.8","Other cardiomyopathies","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I42.9","Cardiomyopathy, unspecified","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I43","Cardiomyopathy in diseases classified elsewhere","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","I25.5 ","Ischaemic cardiomyopathy","1","20210127"),
# MAGIC ("cardiomyopathy","ICD10","O90.3 ","Cardiomyopathy in the puerperium","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)
# MAGIC union all
# MAGIC select *
# MAGIC from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
# MAGIC where name like 'cardiomyopathy'
# MAGIC and terminology like 'SNOMED'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Heart failure

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_HF as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("HF","ICD10","I50","Heart failure","",""),
# MAGIC ("HF","ICD10","I11.0","Hypertensive heart disease with (congestive) heart failure","",""),
# MAGIC ("HF","ICD10","I13.0","Hypertensive heart and renal disease with (congestive) heart failure","",""),
# MAGIC ("HF","ICD10","I13.2","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure","",""),
# MAGIC ("HF","SNOMED","10335000","Chronic right-sided heart failure (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","10633002","Acute congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","42343007","Congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","43736008","Rheumatic left ventricular failure","1","20210127"),
# MAGIC ("HF","SNOMED","48447003","Chronic heart failure (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","56675007","Acute heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","71892000","Cardiac asthma","1","20210127"),
# MAGIC ("HF","SNOMED","79955004","Chronic cor pulmonale","1","20210127"),
# MAGIC ("HF","SNOMED","83105008","Malignant hypertensive heart disease with congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","84114007","Heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","85232009","Left heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","87837008","Chronic pulmonary heart disease","1","20210127"),
# MAGIC ("HF","SNOMED","88805009","Chronic congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","92506005","Biventricular congestive heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","128404006","Right heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","134401001","Left ventricular systolic dysfunction","1","20210127"),
# MAGIC ("HF","SNOMED","134440006","Referral to heart failure clinic","1","20210127"),
# MAGIC ("HF","SNOMED","194767001","Benign hypertensive heart disease with congestive cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","194779001","Hypertensive heart and renal disease with (congestive) heart failure","1","20210127"),
# MAGIC ("HF","SNOMED","194781004","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure","1","20210127"),
# MAGIC ("HF","SNOMED","195111005","Decompensated cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","195112003","Compensated cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","195114002","Acute left ventricular failure","1","20210127"),
# MAGIC ("HF","SNOMED","206586007","Congenital cardiac failure","1","20210127"),
# MAGIC ("HF","SNOMED","233924009","Heart failure as a complication of care (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","275514001","Impaired left ventricular function","1","20210127"),
# MAGIC ("HF","SNOMED","314206003","Refractory heart failure (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","367363000","Right ventricular failure","1","20210127"),
# MAGIC ("HF","SNOMED","407596008","Echocardiogram shows left ventricular systolic dysfunction (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","420300004","New York Heart Association Classification - Class I (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","420913000","New York Heart Association Classification - Class III (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","421704003","New York Heart Association Classification - Class II (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","422293003","New York Heart Association Classification - Class IV (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","426263006","Congestive heart failure due to left ventricular systolic dysfunction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","426611007","Congestive heart failure due to valvular disease (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","430396006","Chronic systolic dysfunction of left ventricle (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","446221000","Heart failure with normal ejection fraction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","698592004","Asymptomatic left ventricular systolic dysfunction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","703272007","Heart failure with reduced ejection fraction (disorder)","1","20210127"),
# MAGIC ("HF","SNOMED","717491000000102","Excepted from heart failure quality indicators - informed dissent (finding)","1","20210127"),
# MAGIC ("HF","SNOMED","760361000000100","Fast track heart failure referral for transthoracic two dimensional echocardiogram","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Angina & Unstable angina
# MAGIC Unstable angina codes are also included in angina - does this need to change?

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_angina as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("angina","ICD10","I20","Angina","",""),
# MAGIC ("angina","SNOMED","10971000087107","Myocardial ischemia during surgery (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960061000119102","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960581000119102","Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960661000119107","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","19057007","Status anginosus (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","194828000","Angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","21470009","Syncope anginosa (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","233821000","New onset angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","25106000","Impending infarction (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","314116003","Post infarct angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371806006","Progressive angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371809004","Recurrent angina status post coronary stent placement (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371812001","Recurrent angina status post directional coronary atherectomy (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","41334000","Angina, class II (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413439005","Acute ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413444003","Acute myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413838009","Chronic ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","429559004","Typical angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","4557003","Preinfarction syndrome (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","59021001","Angina decubitus (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","61490001","Angina, class I (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","712866001","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","791000119109","Angina associated with type II diabetes mellitus (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","85284003","Angina, class III (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","89323001","Angina, class IV (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960141000119102","Angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","15960381000119109","Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","194823009","Acute coronary insufficiency (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","225566008","Ischemic chest pain (finding)","1","20210127"),
# MAGIC ("angina","SNOMED","233819005","Stable angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","233823002","Silent myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","300995000","Exercise-induced angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","315025001","Refractory angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","35928006","Nocturnal angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371807002","Atypical angina (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371808007","Recurrent angina status post percutaneous transluminal coronary angioplasty (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371810009","Recurrent angina status post coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","371811008","Recurrent angina status post rotational atherectomy (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","394659003","Acute coronary syndrome (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","413844008","Chronic myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","414545008","Ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","414795007","Myocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","46109009","Subendocardial ischemia (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","697976003","Microvascular ischemia of myocardium (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","703214003","Silent coronary vasospastic disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","713405002","Subacute ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("angina","SNOMED","87343002","Prinzmetal angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","ICD10","I20.1","Unstable angina","",""),
# MAGIC ("unstable_angina","SNOMED","15960061000119102","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","15960661000119107","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","19057007","Status anginosus (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","25106000","Impending infarction (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","4557003","Preinfarction syndrome (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","712866001","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","315025001","Refractory angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","35928006","Nocturnal angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","371807002","Atypical angina (disorder)","1","20210127"),
# MAGIC ("unstable_angina","SNOMED","394659003","Acute coronary syndrome (disorder)","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pericarditis and Myocarditis

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_pericarditis_myocarditis as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("pericarditis","ICD10","I30","Acute pericarditis","",""),
# MAGIC ("myocarditis","ICD10","I51.4","Myocarditis, unspecified","",""),
# MAGIC ("myocarditis","ICD10","I40","Acute myocarditis","",""),
# MAGIC ("myocarditis","ICD10","I41","Myocarditis in diseases classified elsewhere","","")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stroke_TIA

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view ccu002_03_codelist_stroke_TIA as
# MAGIC select *
# MAGIC from values
# MAGIC 
# MAGIC ("stroke_TIA","ICD10","G45","Transient cerebral ischaemic attacks and related syndromes","",""),
# MAGIC ("stroke_TIA","SNOMED","195206000","Intermittent cerebral ischaemia","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230716006","Anterior circulation transient ischaemic attack","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","266257000","TIA","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230717002","Vertebrobasilar territory transient ischemic attack (disorder)","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230716006","Anterior circulation transient ischaemic attack","1","20210127"),
# MAGIC ("stroke_TIA","SNOMED","230717002","Vertebrobasilar territory transient ischaemic attack","1","20210127")
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID19 infection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SNOMED COVID-19 codes
# MAGIC -- Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# MAGIC -- Covid-1 Status groups:
# MAGIC -- - Lab confirmed incidence
# MAGIC -- - Lab confirmed historic
# MAGIC -- - Clinically confirmed
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_snomed_codes_covid19 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
# MAGIC ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
# MAGIC ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
# MAGIC ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
# MAGIC ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# MAGIC ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
# MAGIC ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
# MAGIC ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
# MAGIC ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
# MAGIC ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
# MAGIC ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
# MAGIC ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
# MAGIC ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# MAGIC ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# MAGIC ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
# MAGIC ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
# MAGIC ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# MAGIC ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
# MAGIC ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
# MAGIC ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
# MAGIC 
# MAGIC AS tab(clinical_code, description, sensitive_status, include_binary, covid_status);

# COMMAND ----------

# MAGIC %md ## COVID19 vaccine

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelist_vaccine_products AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC ('39114911000001105','SNOMED','COVID19_vaccine_AstraZeneca'),
# MAGIC ('39115011000001105','SNOMED','COVID19_vaccine_AstraZeneca'),
# MAGIC ('39115111000001106','SNOMED','COVID19_vaccine_AstraZeneca'),
# MAGIC ('39115711000001107','SNOMED','COVID19_vaccine_Pfizer'),
# MAGIC ('39115611000001103','SNOMED','COVID19_vaccine_Pfizer'),
# MAGIC ('39326911000001101','SNOMED','COVID19_vaccine_Moderna'), 
# MAGIC ('39375411000001104','SNOMED','COVID19_vaccine_Moderna'),
# MAGIC ('1324681000000101','SNOMED','COVID19_vaccine_dose1'),
# MAGIC ('1324691000000104','SNOMED','COVID19_vaccine_dose2'),
# MAGIC ('1324741000000101','SNOMED','COVID19_vaccine_dose1_declined'),
# MAGIC ('1324751000000103','SNOMED','COVID19_vaccine_dose2_declined'),
# MAGIC ('61396006','','COVID19_vaccine_site_left_thigh'),
# MAGIC ('368209003','','COVID19_vaccine_site_right_upper_arm'),
# MAGIC ('368208006','','COVID19_vaccine_site_left_upper_arm'),
# MAGIC ('723980000','','COVID19_vaccine_site_right_buttock'),
# MAGIC ('723979003','','COVID19_vaccine_site_left_buttock'),
# MAGIC ('11207009','','COVID19_vaccine_site_right_thigh'),
# MAGIC ('413294000','','COVID19_vaccine_care_setting_community_health_services'),
# MAGIC ('310065000','','COVID19_vaccine_care_setting_open_access_service'),
# MAGIC ('788007007','','COVID19_vaccine_care_setting_general_practice_service')
# MAGIC AS tab(code, terminology, name)

# COMMAND ----------

# MAGIC %md ## Combine codelists

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelists_unformatted AS
# MAGIC SELECT
# MAGIC   codelist AS name,
# MAGIC   system AS terminology,
# MAGIC   code,
# MAGIC   term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM
# MAGIC   global_temp.ccu002_03_codelist_drugs
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   smoking_status AS name,
# MAGIC   'SNOMED' AS terminology,
# MAGIC   conceptID AS code,
# MAGIC   description AS term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM
# MAGIC   global_temp.ccu002_03_codelist_smoking
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC 'pregnancy_and_birth' AS name,
# MAGIC 'SNOMED' AS terminology,
# MAGIC code,
# MAGIC term,
# MAGIC "" AS code_type,
# MAGIC  "" AS RecordDate
# MAGIC FROM
# MAGIC global_temp.ccu002_03_codelist_pregnancy_birth
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC 'prostate_cancer' AS name,
# MAGIC 'SNOMED' AS terminology,
# MAGIC code,
# MAGIC term,
# MAGIC "" AS code_type,
# MAGIC  "" AS RecordDate
# MAGIC FROM
# MAGIC global_temp.ccu002_03_codelist_prostate_cancer
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC 'depression' AS name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC FROM
# MAGIC global_temp.ccu002_03_codelist_depression
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_BMI_obesity
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_COPD
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_hypertension
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_diabetes
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_cancer
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_liver
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_dementia
# MAGIC union all 
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_ckd
# MAGIC union all
# MAGIC select 
# MAGIC name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu002_03_codelist_ami
# MAGIC union all
# MAGIC select *
# MAGIC from global_temp.ccu002_03_codelist_vt
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_DVT_ICVT
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_PE
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_stroke_IS
# MAGIC union all
# MAGIC select 
# MAGIC new_name AS name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu002_03_codelist_stroke_NOS
# MAGIC union all
# MAGIC select 
# MAGIC new_name AS name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu002_03_codelist_stroke_SAH
# MAGIC union all
# MAGIC select 
# MAGIC name,
# MAGIC terminology,
# MAGIC code,
# MAGIC term,
# MAGIC code_type,
# MAGIC RecordDate
# MAGIC from global_temp.ccu002_03_codelist_stroke_HS
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_thrombophilia
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_TCP
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_retinal_infarction
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_arterial_embolism
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_DIC
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_mesenteric_thrombus
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_spinal_stroke
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_fracture
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_artery_dissect
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_life_arrhythmias
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_cardiomyopathy
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_HF
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_angina
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_pericarditis_myocarditis
# MAGIC union all
# MAGIC select * 
# MAGIC from global_temp.ccu002_03_codelist_stroke_TIA
# MAGIC union all
# MAGIC SELECT
# MAGIC   'GDPPR_confirmed_COVID' AS name,
# MAGIC   'SNOMED' AS terminology,
# MAGIC   clinical_code AS code,
# MAGIC   description AS term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC from global_temp.ccu002_03_codelist_snomed_codes_covid19
# MAGIC UNION ALL 
# MAGIC SELECT
# MAGIC   name,
# MAGIC   terminology,
# MAGIC   code,
# MAGIC   "" AS term,
# MAGIC   "" AS code_type,
# MAGIC   "" AS RecordDate
# MAGIC FROM global_temp.ccu002_03_codelist_vaccine_products

# COMMAND ----------

# MAGIC %md ## Save codelists

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_03_codelists AS
# MAGIC SELECT name,
# MAGIC        terminology,
# MAGIC        REGEXP_REPLACE(code,'[\.]','') AS code, -- remove dots from codes
# MAGIC        term,
# MAGIC        code_type
# MAGIC FROM global_temp.ccu002_03_codelists_unformatted

# COMMAND ----------

drop_table("ccu002_03_codelists")

# COMMAND ----------

create_table("ccu002_03_codelists")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT name, terminology
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_03_codelists
# MAGIC WHERE name RLIKE 'carditis'
