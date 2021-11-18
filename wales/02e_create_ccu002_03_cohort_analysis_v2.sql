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
--
-- Dose 1 
--
--------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS('sailwWMCCV.ccu002_03_cohort_analysis_dose1');

CREATE TABLE sailwWMCCV.ccu002_03_cohort_analysis_dose1 (
	NHS_NUMBER_DEID		      		BIGINT NOT NULL,
    
    vaccination_dose1_date          DATE,
    vaccination_dose1_product      	VARCHAR(50),
--  vaccination_dose2_date          DATE,
--  vaccination_dose2_product      	VARCHAR(50),
	
	cov_age							Decimal(13,0),
	cov_sex							SMALLINT,
	cov_ethnicity					VARCHAR(225),
	lsoa2011						VARCHAR(10), ---this is lsoa_2011_cd
	cov_region						VARCHAR(10),
	cov_deprivation					SMALLINT, ---    wimd_2019_rank, wimd_2019_decile, wimd_2019_quintile
	cov_myo_or_pericarditis			integer, 
	cov_prior_covid19				integer, 
	
	out_death					  	date, 
	out_any_myo_or_pericarditis 	date, 
    PRIMARY KEY (NHS_NUMBER_DEID)
	) DISTRIBUTE BY HASH (NHS_NUMBER_DEID);
	

GRANT ALL ON TABLE sailwWMCCV.ccu002_03_cohort_analysis_dose1 TO ROLE NRDASAIL_SAIL_0911_ANALYST;

/* initial insert of c20, mortality and vaccination info */

INSERT INTO sailwWMCCV.ccu002_03_cohort_analysis_dose1 (
	NHS_NUMBER_DEID		    		,
    
    vaccination_dose1_date          ,
    vaccination_dose1_product      	,
 	
	cov_age							,
	cov_sex							,
	cov_ethnicity					,
	lsoa2011						,
--	cov_region						,				
	cov_deprivation					,
--	cov_myo_or_pericarditis			, 
	cov_prior_covid19				, 
	out_death
	
)
SELECT DISTINCT 
		base.ALF_E  AS NHS_NUMBER_DEID,
		vacc.VACC_FIRST_DATE 	AS vaccination_dose1_date,
		vacc.VACC_FIRST_NAME 	AS vaccination_dose1_product,

		base.COV_AGE 				,
		base.COV_SEX				,
		aa.ETH_ONS AS COV_ETHNICITY	,
		base.LSOA2011 				,
--		'Wales'		 AS   cov_region,
		vacc.WIMD_2019_QUINTILE AS cov_deprivation, 
		CASE WHEN vacc.positive_test_date <= vacc.vacc_first_date THEN 1 ELSE 0 END AS cov_prior_covid19,
		CASE WHEN base.death_date >= '2020-12-08' THEN base.death_date ELSE NULL END AS out_death 

		FROM 
		sailwWMCCV.ccu002_03_cohort_base base
		LEFT JOIN 
		sailwWMCCV.ccu002_03_cohort_vaccine vacc
		ON 
		base.ALF_E = vacc.ALF_E
		LEFT JOIN 
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
		) aa 
		ON 
		base.ALF_E = aa.ALF_E; 

------merging outcome related flags IN: 
--dose 1
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis_dose1 taba
USING 
(
	SELECT
	DISTINCT 
	a.ALF_E,
	min(a.ADMIS_DT) dt
	FROM 
	sailwWMCCV.ccu002_03_cohort_outcome_pedw a 
	WHERE admis_dt < '2020-12-08'
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.NHS_NUMBER_DEID =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.cov_myo_or_pericarditis=1;

--------------------------------------------
--outcome
--------------------------------------------
------merging outcome related flags IN: 
--dose 1
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis_dose1 taba
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
	WHERE a.admis_dt >= '2020-12-08'  
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.NHS_NUMBER_DEID =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.OUT_ANY_MYO_OR_PERICARDITIS=tabb.dt;

COMMIT; 


UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose1
SET cov_region=case WHEN cov_region IS NULL THEN 'Wales' ELSE 'UNKNOWN' END; 

UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose1
SET cov_ethnicity=CASE WHEN cov_ethnicity IS NULL THEN 'UNKNOWN' ELSE COV_ETHNICITY END; 

COMMIT; 
----------------------------------------------
--
--Dose 2
--
----------------------------------------------
CALL FNC.DROP_IF_EXISTS('sailwWMCCV.ccu002_03_cohort_analysis_dose2');

CREATE TABLE sailwWMCCV.ccu002_03_cohort_analysis_dose2 (
	NHS_NUMBER_DEID		      		BIGINT NOT NULL,
    
    vaccination_dose1_date          DATE,
    vaccination_dose1_product      	VARCHAR(50),
  	vaccination_dose2_date          DATE,
	vaccination_dose2_product      	VARCHAR(50),
	
	cov_age							Decimal(13,0),
	cov_sex							SMALLINT,
	cov_ethnicity					VARCHAR(225),
	lsoa2011						VARCHAR(10), ---this is lsoa_2011_cd
	cov_region						VARCHAR(10),
	cov_deprivation					SMALLINT, ---    wimd_2019_rank, wimd_2019_decile, wimd_2019_quintile
	cov_myo_or_pericarditis			integer, 
	cov_prior_covid19				integer, 
	
	out_death					  	date, 
	out_any_myo_or_pericarditis 	date, 
    PRIMARY KEY (NHS_NUMBER_DEID)
	) DISTRIBUTE BY HASH (NHS_NUMBER_DEID);
	

GRANT ALL ON TABLE sailwWMCCV.ccu002_03_cohort_analysis_dose2 TO ROLE NRDASAIL_SAIL_0911_ANALYST;

/* initial insert of c20, mortality and vaccination info */

INSERT INTO sailwWMCCV.ccu002_03_cohort_analysis_dose2 (
 	NHS_NUMBER_DEID					,
    
    vaccination_dose1_date          ,
    vaccination_dose1_product      	,
    vaccination_dose2_date          ,
    vaccination_dose2_product      	,
 	
	cov_age							,
	cov_sex							,
	cov_ethnicity					,
	lsoa2011						,
--	cov_region						,				
	cov_deprivation					,
--	cov_myo_or_pericarditis			, 
	cov_prior_covid19				, 
	out_death
	
)
SELECT DISTINCT 
		base.ALF_E AS NHS_NUMBER_DEID ,
		vacc.VACC_FIRST_DATE 	AS vaccination_dose1_date,
		vacc.VACC_FIRST_NAME 	AS vaccination_dose1_product,
		vacc.VACC_SECOND_DATE 	AS vaccination_dose1_date,
		vacc.VACC_SECOND_NAME 	AS vaccination_dose1_product,

		base.COV_AGE 					,
		base.COV_SEX					,
		aa.eth_ons     AS cov_ethnicity ,
--		'Wales' 	   AS cov_region	,
		base.LSOA2011 					,
		vacc.WIMD_2019_QUINTILE AS cov_deprivation, 
		CASE WHEN vacc.positive_test_date <= vacc.vacc_first_date THEN 1 ELSE 0 END AS cov_prior_covid19,
		CASE WHEN base.death_date >= '2020-12-08' THEN base.death_date ELSE NULL END AS out_death 

		FROM 
		sailwWMCCV.ccu002_03_cohort_base base
		LEFT JOIN 
		sailwWMCCV.ccu002_03_cohort_vaccine vacc
		ON 
		base.ALF_E = vacc.ALF_E
		LEFT JOIN 
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
		) aa 
		ON 
		base.ALF_E = aa.ALF_E; 
 

------merging outcome related flags IN: 
--prior
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis_dose2 taba
USING 
(
	SELECT
	DISTINCT 
	a.ALF_E,
	min(a.ADMIS_DT) dt
	FROM 
	sailwWMCCV.ccu002_03_cohort_outcome_pedw a 
	WHERE admis_dt < '2020-12-08'
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.NHS_NUMBER_DEID =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.cov_myo_or_pericarditis=1;

--------------------------------------------
--outcome
--------------------------------------------
------merging outcome related flags IN: 
--dose 1
MERGE INTO sailwWMCCV.ccu002_03_cohort_analysis_dose2 taba
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
	WHERE a.admis_dt >= b.VACCINATION_DOSE1_DATE  
	GROUP BY 
	a.ALF_E 
)tabb
ON 
taba.NHS_NUMBER_DEID =tabb.alf_e
WHEN MATCHED THEN UPDATE SET 
taba.OUT_ANY_MYO_OR_PERICARDITIS=tabb.dt;

COMMIT; 


SELECT * FROM sailwWMCCV.ccu002_03_cohort_analysis_dose2;


UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose2
SET cov_region=case WHEN cov_region IS NULL THEN 'Wales' ELSE 'UNKNOWN' END; 

UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose2
SET cov_ethnicity=CASE WHEN cov_ethnicity IS NULL THEN 'UNKNOWN' ELSE COV_ETHNICITY END; 

SELECT DISTINCT  cov_region FROM sailwWMCCV.ccu002_03_cohort_analysis_dose2;
SELECT DISTINCT  cov_ethnicity FROM sailwWMCCV.ccu002_03_cohort_analysis_dose2;


UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose1
SET cov_deprivation=CASE WHEN cov_deprivation IS NULL THEN '999' ELSE cov_deprivation END;
UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose2
SET cov_deprivation=CASE WHEN cov_deprivation IS NULL THEN '999' ELSE cov_deprivation END;


UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose1
SET cov_myo_or_pericarditis = CASE WHEN cov_myo_or_pericarditis IS NULL THEN 0 ELSE cov_myo_or_pericarditis END; 
UPDATE sailwWMCCV.ccu002_03_cohort_analysis_dose2
SET cov_myo_or_pericarditis = CASE WHEN cov_myo_or_pericarditis IS NULL THEN 0 ELSE cov_myo_or_pericarditis END; 