--------------------------------------------------------------------------------
--Project: 
--BY: fatemeh.torabi@swansea.ac.uk - Stuart Bedeston - Ashley Akbari - Hoda Abassizanjani - Jane Lyons
--DT: 2021-10-06
--aim: to create ccu002-03 cohort and run analysis on it
--------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS('SAILWWMCCV.ccu002_03_cohort_base');

--************************************************************************************************
-- Script:       3-5_CCU002_cohort.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Creating the analysis table containing individual level information for project CCU002

-- Created by Hoda Abbasizanjani      2021-05-11
-- Health Data Research UK, Swansea University
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_03_START_DATE DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_03_END_DATE DATE DEFAULT '2020-12-07';

-- ***********************************************************************************************
-- Create CCU002 cohort
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.ccu002_03_cohort_base (
    alf_e                            bigint,
    wob                              date,
    death_date                       date,
    lsoa2011                         char(10),
    rural_urban                      char(47),
    wimd2019                         int,
    lost_to_followup                 char(1),
    lost_to_followup_date            date,
    care_home                        smallint,
    prac_cd_e                        bigint,
    gp_coverage_end_date             date,
    consultation_rate                int,
    pregnancy                        smallint,
    unique_bnf_codes                 int,
    primary_covid19_date             date,
    primary_exposure                 char(1),
    secondary_exposure               char(1),
    secondary_date                   date,
    secondary_ehr_confirmed          char(1),
    secondary_desc                   char(20),
    cov_sex                          char(1),
    cov_age                          int,
    cov_ethnicity                    char(15),
    exp_confirmed_covid19_date       date,
    exp_confirmed_covid_phenotype    char(30),
    covid_status                     char(30),
    out_icvt_pregnancy               date,
    out_artery_dissect               date,
    out_angina                       date,
    out_other_dvt                    date,
    out_dvt_icvt                     date,
    out_dvt_pregnancy                date,
    out_dvt_dvt                      date,
    out_fracture                     date,
    out_thrombocytopenia             date,
    out_life_arrhythmia              date,
    out_pericarditis                 date,
    out_ttp                          date,
    out_mesenteric_thrombus          date,
    out_dic                          date,
    out_myocarditis                  date,
    out_stroke_tia                   date,
    out_stroke_isch                  date,
    out_other_arterial_embolism      date,
    out_unstable_angina              date,
    out_pe                           date,
    out_ami                          date,
    out_hf                           date,
    out_portal_vein_thrombosis       date,
    out_cardiomyopathy               date,
    out_stroke_sah_hs                date,
    out_arterial_event               date,
    out_venous_event                 date,
    out_haematological_event         date,
    out_dvt_event                    date,
    out_icvt_event                   date,
    cov_smoking_status               char(20),
    cov_ever_ami                     smallint,
    cov_ever_pe_vt                   smallint,
    cov_ever_icvt                    smallint,
    cov_ever_all_stroke              smallint,
    cov_ever_thrombophilia           smallint,
    cov_ever_tcp                     smallint,
    cov_ever_dementia                smallint,
    cov_ever_liver_disease           smallint,
    cov_ever_ckd                     smallint,
    cov_ever_cancer                  smallint,
    cov_surgery_lastyr               smallint,
    cov_ever_hypertension            smallint,
    cov_ever_diabetes                smallint,
    cov_ever_obesity                 smallint,
    cov_ever_depression              smallint,
    cov_ever_copd                    smallint,
    cov_deprivation                  smallint,
    cov_region                       char(50),
    cov_antiplatelet_meds            smallint,
    cov_lipid_meds                   smallint,
    cov_anticoagulation_meds         smallint,
    cov_cocp_meds                    smallint,
    cov_hrt_meds                     smallint,
    cov_n_disorder                   int,
    cov_ever_other_arterial_embolism smallint,
    cov_ever_dic                     smallint,
    cov_ever_mesenteric_thrombus     smallint,
    cov_ever_artery_dissect          smallint,
    cov_ever_life_arrhythmia         smallint,
    cov_ever_cardiomyopathy          smallint,
    cov_ever_hf                      smallint,
    cov_ever_pericarditis            smallint,
    cov_ever_myocarditis             smallint,
    cov_unique_bnf_chaps             int
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.ccu002_03_cohort_base;
TRUNCATE TABLE SAILWWMCCV.ccu002_03_cohort_base IMMEDIATE;

INSERT INTO SAILWWMCCV.ccu002_03_cohort_base (alf_e, cov_sex, wob, death_date, cov_age, lsoa2011, wimd2019,
                                          rural_urban, gp_coverage_end_date, care_home)
    SELECT alf_e,
           gndr_cd,
           wob,
           --dod,
           dod_jl,  -- combined DOD
           der_age_,
           lsoa2011_inception,
           wimd2019_quintile_inception,
           urban_rural_inception,
           gp_coverage_end_date,
           CASE WHEN carehome_ralf_inception IS NOT NULL THEN 1
           ELSE 0 END
    FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914;
-- ***********************************************************************************************
-- Update covariates using SAILWWMCCV.CCU002_COVARIATES
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.consultation_rate                = src.consultation_rate,
    tgt.unique_bnf_codes                 = src.unique_medications,
    tgt.pregnancy                        = CASE WHEN src.pregnancy = 1 THEN 1 ELSE 0 END,
    tgt.cov_smoking_status               = CASE WHEN src.smoking_status IS NULL THEN 'Missing' ELSE src.smoking_status END,
    tgt.cov_ever_ami                     = CASE WHEN src.ami = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_pe_vt                   = CASE WHEN src.pe_vt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_icvt                    = CASE WHEN src.dvt_icvt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_all_stroke              = CASE WHEN src.all_stroke = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_thrombophilia           = CASE WHEN src.thrombophilia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_tcp                     = CASE WHEN src.tcp = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_dementia                = CASE WHEN src.dementia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_liver_disease           = CASE WHEN src.liver_disease = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_ckd                     = CASE WHEN src.ckd = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_cancer                  = CASE WHEN src.cancer = 1 THEN 1 ELSE 0 END,
    tgt.cov_surgery_lastyr               = CASE WHEN src.surgery = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_hypertension            = CASE WHEN src.hypertension_diag = 1 OR src.hypertension_medication = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_diabetes                = CASE WHEN src.diabetes_medication = 1 OR src.diabetes_diag = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_obesity                 = CASE WHEN src.obesity = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_depression              = CASE WHEN src.depression = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_copd                    = CASE WHEN src.copd = 1 THEN 1 ELSE 0 END,
    tgt.cov_antiplatelet_meds            = CASE WHEN src.antiplatelet = 1 THEN 1 ELSE 0 END,
    tgt.cov_lipid_meds                   = CASE WHEN src.lipid_lowering = 1 THEN 1 ELSE 0 END,
    tgt.cov_anticoagulation_meds         = CASE WHEN src.anticoagulant = 1 THEN 1 ELSE 0 END,
    tgt.cov_cocp_meds                    = CASE WHEN src.cocp = 1 THEN 1 ELSE 0 END,
    tgt.cov_hrt_meds                     = CASE WHEN src.hrt = 1 THEN 1 ELSE 0 END,
    tgt.cov_n_disorder                   = src.n_disorder,
    tgt.cov_ever_other_arterial_embolism = CASE WHEN src.other_arterial_embolism = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_dic                     = CASE WHEN src.dic = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_mesenteric_thrombus     = CASE WHEN src.mesenteric_thrombus = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_artery_dissect          = CASE WHEN src.artery_dissect = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_life_arrhythmia         = CASE WHEN src.life_arrhythmia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_cardiomyopathy          = CASE WHEN src.cardiomyopathy = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_hf                      = CASE WHEN src.hf = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_pericarditis            = CASE WHEN src.pericarditis = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_myocarditis             = CASE WHEN src.myocarditis = 1 THEN 1 ELSE 0 END,
    tgt.cov_unique_bnf_chaps             = src.unique_bnf_chapters
FROM SAILWWMCCV.CCU002_COVARIATES_20210914 src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.ccu002_03_cohort_base
SET cov_deprivation = wimd2019
WHERE wimd2019 IS NOT NULL;

UPDATE SAILWWMCCV.ccu002_03_cohort_base
SET cov_region = 'Wales'  -- ?
WHERE alf_e IS NOT NULL;

SELECT count(*) FROM SAILWWMCCV.ccu002_03_cohort_base WHERE cov_unique_bnf_chaps > 0;
-- ***********************************************************************************************
-- Update outcomes using SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_ami = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'AMI') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_stroke_isch = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'STROKE_ISCH') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_stroke_tia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'STROKE_TIA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_icvt_pregnancy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'ICVT_PREGNANCY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_artery_dissect = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'ARTERY_DISSECT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_angina = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'ANGINA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_unstable_angina = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'UNSTABLE_ANGINA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_other_dvt = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'OTHER_DVT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_dvt_icvt = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'DVT_ICVT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_dvt_pregnancy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'DVT_PREGNANCY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_dvt_dvt = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'DVT_DVT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_fracture = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'FRACTURE') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_thrombocytopenia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'THROMBOCYTOPENIA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_life_arrhythmia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'LIFE_ARRHYTHM') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_pericarditis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'PERICARDITIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_ttp = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'TTP') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_mesenteric_thrombus = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'MESENTERIC_THROMBUS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_dic = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'DIC') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_myocarditis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'MYOCARDITIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_other_arterial_embolism = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'ARTERIAL_EMBOLISM_OTHR') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_pe = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'PE') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_hf = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'HF') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_portal_vein_thrombosis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'PORTAL_VEIN_THROMBOSIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_cardiomyopathy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'CARDIOMYOPATHY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_stroke_sah_hs = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
      WHERE name = 'STROKE_SAH_HS') src
WHERE tgt.alf_e = src.alf_e;
-- ***********************************************************************************************
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_dvt_event = src.out_dvt_event
FROM (SELECT alf_e,
             min(record_date) AS out_dvt_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
            WHERE name IN ('DVT_DVT', 'DVT_PREGNANCY')
            )
      GROUP BY alf_e    
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_icvt_event = src.out_icvt_event
FROM (SELECT alf_e,
             min(record_date) AS out_icvt_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
            WHERE name IN ('DVT_ICVT','ICVT_PREGNANCY')
            )
      GROUP BY alf_e    
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_arterial_event = src.out_arterial_event
FROM (SELECT alf_e,
             min(record_date) AS out_arterial_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
            WHERE name IN ('AMI','STROKE_ISCH','ARTERIAL_EMBOLISM_OTHR')
            )
       GROUP BY alf_e    
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_venous_event = src.out_venous_event
FROM (SELECT alf_e,
             min(record_date) AS out_venous_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
            WHERE name IN ('PE', 'OTHER_DVT', 'DVT_ICVT', 'DVT_PREGNANCY', 'DVT_DVT',
                           'ICVT_PREGNANCY','PORTAL_VEIN_THROMBOSIS')
            )
       GROUP BY alf_e    
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
 UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.out_haematological_event = src.out_haematological_event
FROM (SELECT alf_e,
             min(record_date) AS out_haematological_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
            WHERE name IN ('DIC', 'TTP', 'THROMBOCYTOPENIA')
            )
      GROUP BY alf_e    
      ) src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Update exposure using SAILWWMCCV.CCU002_COVID19_COHORT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.exp_confirmed_covid19_date = src.covid19_confirmed_date,
    tgt.exp_confirmed_covid_phenotype = src.covid19_hospitalisation_phenotype
FROM SAILWWMCCV.CCU002_COVID19_COHORT_20210914 src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.ccu002_03_cohort_base
SET covid_status = CASE WHEN exp_confirmed_covid_phenotype = 'hospitalised' THEN 'Hospitalised'
                        WHEN exp_confirmed_covid_phenotype = 'non-hospitalised' THEN 'Non-hospitalised'
                        ELSE 'No COVID-19'
                   END
WHERE alf_e IS NOT NULL;
-- ***********************************************************************************************
-- Other variables
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.primary_covid19_date = src.first_date,
    tgt.primary_exposure = 1
FROM (SELECT alf_e,
             first_date
      FROM SAILWWMCCV.CCU002_COVID19_PRIMARY_EXPOSURE_20210914
      WHERE first_date >= SAILWWMCCV.CCU002_START_DATE --'2020-01-01'
      AND first_date <= SAILWWMCCV.CCU002_END_DATE) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.secondary_exposure = 1,
    tgt.secondary_date = src.secondary_date,
    tgt.secondary_ehr_confirmed = src.secondary_ehr_confirmed,
    tgt.secondary_desc = src.secondary_desc
FROM (SELECT alf_e,
             secondary_date,
             secondary_ehr_confirmed,
             secondary_desc
      FROM SAILWWMCCV.CCU002_COVID19_SECONDARY_EXPOSURE_20210914
      WHERE secondary_date >= SAILWWMCCV.CCU002_START_DATE --'2020-01-01'
      AND secondary_date <= SAILWWMCCV.CCU002_END_DATE) src
WHERE tgt.alf_e = src.alf_e;

--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.ccu002_03_cohort_base tgt
SET tgt.cov_n_disorder = src.n_disorder
FROM SAILWWMCCV.CCU002_COVARIATES_20210914 src
WHERE tgt.alf_e = src.alf_e;

--SELECT count(*) FROM SAILWWMCCV.ccu002_03_cohort_base
SELECT max(cov_n_disorder) FROM SAILWWMCCV.ccu002_03_cohort_base -- 34
WHERE cov_n_disorder > 0;

SELECT count(*) FROM SAILWWMCCV.ccu002_03_cohort_base
--WHERE cov_n_disorder IS NULL;
WHERE cov_n_disorder > 0;

--------------------------------------------------------------------------------------------------
MERGE INTO SAILWWMCCV.ccu002_03_cohort_base A
USING 
(
SELECT DISTINCT alf_e, 
		CASE 
		WHEN ehrd_ec_ons = 1 THEN 'White'
		WHEN ehrd_ec_ons = 2 THEN 'Mixed'
		WHEN ehrd_ec_ons = 3 THEN 'Asian'
		WHEN ehrd_ec_ons = 4 THEN 'Black'
		WHEN ehrd_ec_ons = 5 THEN 'Other'
		ELSE NULL END AS ETH_ONS
		FROM SAILWWMCCV.wmcc_comb_ethn_ehrd_ec 
) B
ON 
A.ALF_E = B.ALF_E
WHEN MATCHED THEN UPDATE SET
A.COV_ETHNICITY = B.ETH_ONS;
-- ***********************************************************************************************
-- Apply further exclusions
DELETE FROM SAILWWMCCV.ccu002_03_cohort_base
WHERE cov_age >= 110 -- 0
OR death_date < exp_confirmed_covid19_date
OR (cov_sex=1 AND cov_cocp_meds=1)
OR (cov_sex=1 AND cov_hrt_meds=1);

-- ***********************************************************************************************
-- Remove anyone whose only COVID record is a death date from COVID (further excludes people who
-- dies on/after their minimum day of exposure)
DELETE FROM SAILWWMCCV.ccu002_03_cohort_base
--SELECT count(*) FROM SAILWWMCCV.ccu002_03_cohort_base
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.WMCC_DEATH_COVID19 WHERE dod <= SAILWWMCCV.CCU002_END_DATE)
AND alf_e NOT IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_COVID19_COHORT_20210914);

-------------------------------------------------------------------------------------------------
--adding outcome:
ALTER TABLE SAILWWMCCV.ccu002_03_cohort_base
ADD COLUMN out_myo_pericarditis date 

MERGE INTO SAILWWMCCV.ccu002_03_cohort_base a 
USING 
(
SELECT DISTINCT ALF_E, min(admis_dt) ADMIS_DT FROM 
SAILWWMCCV.CCU002_03_COHORT_OUTCOME_PEDW 
WHERE ADMIS_DT >= '2000-01-01'
GROUP BY ALF_E 
)b 
ON 
a.ALF_E =b.alf_e
WHEN MATCHED THEN UPDATE SET 
a.out_myo_pericarditis=b.admis_dt;

COMMIT; 