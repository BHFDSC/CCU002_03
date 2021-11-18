--------------------------------------------------------------------------------
--Project: 
--BY: fatemeh.torabi@swansea.ac.uk - Stuart Bedeston - Ashley Akbari - Hoda Abassizanjani - Jane Lyons
--DT: 2021-10-06
--aim: create ccu002_03 cohort_analysis
--------------------------------------------------------------------------------
/*
   cov_dose1* variables are defined prior to 8 December 2020
   cov dose2* variables are defined between dose1 and dose2 date
   out_dose1* variables are the first event that occures on or after 8 December 2020
   out_dose2* variables are the first event that occures on or after dose 1 date 
    
 */   
--------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS('sailwWMCCV.ccu002_03_cohort_analysis');

CREATE TABLE sailwWMCCV.ccu002_03_cohort_analysis (
	alf_e                    		BIGINT NOT NULL,
    
    vaccination_dose1_date          DATE,
    vaccination_dose1_product      	VARCHAR(50),
    vaccination_dose2_date          DATE,
    vaccination_dose2_product      	VARCHAR(50),
	
	cov_dose1_age					Decimal(13,0),
	cov_dose1_sex					SMALLINT,
	cov_dose1_ethnicity				VARCHAR(225),
	cov_dose1_lsoa					VARCHAR(10), ---this is lsoa_2011_cd
	cov_dose1_region				VARCHAR(10),
	cov_dose1_deprivation			SMALLINT, ---    wimd_2019_rank, wimd_2019_decile, wimd_2019_quintile
	cov_dose1_myo_or_pericarditis	integer, 
	cov_dose1_prior_covid19			integer, 

	cov_dose2_age					Decimal(23,0),
	cov_dose2_sex					SMALLINT,
	cov_dose2_ethnicity				VARCHAR(225),
	cov_dose2_lsoa					VARCHAR(20), ---this is lsoa_2022_cd
	cov_dose2_region				VARCHAR(10),
	cov_dose2_deprivation			SMALLINT,
	cov_dose2_myo_or_pericarditis	integer, 
	cov_dose2_prior_covid19			integer, 
	
	out_death						  date, 
	out_dose1_any_myo_or_pericarditis date, 
    out_dose2_any_myo_or_pericarditis date, 
    PRIMARY KEY (alf_e)
	) DISTRIBUTE BY HASH (alf_e);
	

GRANT ALL ON TABLE sailwWMCCV.ccu002_03_cohort_analysis TO ROLE NRDASAIL_SAIL_0911_ANALYST;

/* initial insert of c20, mortality and vaccination info */

INSERT INTO sailwWMCCV.ccu002_03_cohort_analysis (
 	alf_e                    		,
    
    vaccination_dose1_date          ,
    vaccination_dose1_product      	,
    vaccination_dose2_date          ,
    vaccination_dose2_product      	,
	
	cov_dose1_age					,
	cov_dose1_sex					,
	cov_dose1_ethnicity				,
	cov_dose1_lsoa					,
	cov_dose1_region				,
	cov_dose1_deprivation			,
--	cov_dose1_myo_or_pericarditis	, 
	cov_dose1_prior_covid19			, 

	cov_dose2_age					,
	cov_dose2_sex					,
	cov_dose2_ethnicity				,
	cov_dose2_lsoa					,
	cov_dose2_region				,
	cov_dose2_deprivation			,
--	cov_dose2_myo_or_pericarditis	, 
	cov_dose2_prior_covid19			,
	out_death
	
)
SELECT DISTINCT 
		base.ALF_E ,
		vacc.VACC_FIRST_DATE 	AS vaccination_dose1_date,
		vacc.VACC_FIRST_NAME 	AS vaccination_dose1_product,
		vacc.VACC_SECOND_DATE 	AS vaccination_dose2_date,
		vacc.VACC_SECOND_NAME  	AS vaccination_dose2_date, 

		base.COV_AGE 			AS cov_dose1_age,
		base.COV_SEX 			AS cov_dose1_sex,
		base.COV_ETHNICITY 		AS cov_dose1_ethnicity,
		base.LSOA2011 			AS cov_dose1_lsoa,
		'Wales' 				AS cov_doe1_region,
		vacc.WIMD_2019_QUINTILE AS cov_dose_1_deprivation, 
		CASE WHEN vacc.positive_test_date <= vacc.vacc_first_date THEN 1 ELSE 0 END AS cov_dose1_prior_covid19,
		
		base.COV_AGE 			AS cov_dose2_age,
		base.COV_SEX 			AS cov_dose2_sex,
		base.COV_ETHNICITY 		AS cov_dose2_ethnicity,
		base.LSOA2011 			AS cov_dose2_lsoa,
		'Wales' 				AS cov_doe2_region,
		vacc.WIMD_2019_QUINTILE AS cov_dose_2_deprivation,
		CASE WHEN vacc.positive_test_date between vacc.vacc_first_date AND vacc.vacc_second_date THEN 1 ELSE 0 END AS cov_dose1_prior_covid19, 
		CASE WHEN base.death_date >= '2020-12-08' THEN base.death_date ELSE NULL END AS out_death 

		FROM 
		sailwWMCCV.ccu002_03_cohort_base base
		LEFT JOIN 
		sailwWMCCV.ccu002_03_cohort_vaccine vacc
		ON 
		base.ALF_E = vacc.ALF_E; 

------merging outcome related flags IN: 
--dose 1
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis taba
USING 
(
	SELECT
	DISTINCT 
	a.ALF_E,
	min(a.ADMIS_DT) dt
	FROM 
	sailwWMCCV.ccu002_03_cohort_outcome_pedw a 
	WHERE admis_dt <= '2020-12-08'
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.ALF_E =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.cov_dose1_myo_or_pericarditis=1;

--dose 2
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis taba
USING 
(
	SELECT
	DISTINCT 
	a.ALF_E,
	min(a.ADMIS_DT) dt
	FROM 
	sailwWMCCV.ccu002_03_cohort_outcome_pedw a 
	LEFT JOIN 
	sailwWMCCV.ccu002_03_cohort_analysis b 
	ON 
	a.ALF_E =b.ALF_E 
	WHERE a.admis_dt BETWEEN b.VACCINATION_DOSE1_DATE AND b.VACCINATION_DOSE2_DATE 
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.ALF_E =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.cov_dose2_myo_or_pericarditis=1;

--------------------------------------------
--outcome
--------------------------------------------
------merging outcome related flags IN: 
--dose 1
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis taba
USING 
(
	SELECT
	DISTINCT 
	a.ALF_E,
	min(a.ADMIS_DT) dt
	FROM 
	sailwWMCCV.ccu002_03_cohort_outcome_pedw a 
	LEFT JOIN 
	sailwWMCCV.ccu002_03_cohort_analysis b 
	ON 
	a.ALF_E =b.ALF_E 
	WHERE a.admis_dt BETWEEN '2020-12-08' AND b.VACCINATION_DOSE2_DATE 
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.ALF_E =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.OUT_DOSE1_ANY_MYO_OR_PERICARDITIS=tabb.dt;

--dose 2
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis taba
USING 
(
	SELECT
	DISTINCT 
	a.ALF_E,
	min(a.ADMIS_DT) dt
	FROM 
	sailwWMCCV.ccu002_03_cohort_outcome_pedw a 
	LEFT JOIN 
	sailwWMCCV.ccu002_03_cohort_analysis b 
	ON 
	a.ALF_E =b.ALF_E 
	WHERE a.admis_dt >= b.VACCINATION_DOSE2_DATE 
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.ALF_E =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.OUT_DOSE2_ANY_MYO_OR_PERICARDITIS=tabb.dt;

COMMIT; 

SELECT * FROM sailwWMCCV.ccu002_03_cohort_analysis;