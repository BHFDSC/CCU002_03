--------------------------------------------------------------------------------
--Project: 
--BY: fatemeh.torabi@swansea.ac.uk
--DT: 2021-09-23
--aim: to identify how many hospitalisation exists in Welsh population
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS('sailwWMCCV.ccu002_03_cohort_outcome_pedw');

CREATE TABLE sailwWMCCV.ccu002_03_cohort_outcome_pedw AS (
select 
				sp.ALF_E, sp.gndr_cd,sp.admis_dt, 
				sp.admis_mthd_cd,
				sp.disch_dt,sp.disch_mthd_cd,

				ep.epi_num, ep.epi_str_dt,ep.epi_end_dt, ep.age_epi_str_yr,
				
				SUBSTR(UPPER(ep.Diag_cd_123),1,3)||SUBSTR(UPPER(ep.DIAG_CD_4),1,1) epi_diag_1234 ,

				diag.diag_cd_1234, 
				
				phen.CODE, phen.DESC, phen.name
				
			from 
				SAILWWMCCV.WMCC_PEDW_DIAG_20210805 diag
right outer join
				(
				SELECT * FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS 
				UNION 
				SELECT * FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
				) phen
		on
				diag.diag_cd_1234=phen.CODE

LEFT JOIN
				SAILWWMCCV.WMCC_PEDW_SPELL_20210805 sp
		ON			
				diag.prov_unit_cd=sp.prov_unit_cd
				and
				diag.SPELL_NUM_E=sp.SPELL_NUM_E
				AND 
				sp.alf_sts_cd in ('1','4','39')
left join
				SAILWWMCCV.WMCC_PEDW_EPISODE_20210805 ep
				on 
				sp.prov_unit_cd=ep.prov_unit_cd
				AND 
				sp.SPELL_NUM_E=ep.SPELL_NUM_E
				AND 
				diag.epi_num=ep.epi_num
WHERE SP.ALF_E  IS NOT NULL 
AND 
SP.GNDR_CD IS NOT NULL 
AND 
SP.ADMIS_DT IS NOT NULL 
)WITH NO DATA; 


INSERT INTO sailwWMCCV.ccu002_03_cohort_outcome_pedw
select 
				sp.ALF_E, sp.gndr_cd,sp.admis_dt, 
				sp.admis_mthd_cd,
				sp.disch_dt,sp.disch_mthd_cd,

				ep.epi_num, ep.epi_str_dt,ep.epi_end_dt, ep.age_epi_str_yr,
				
				SUBSTR(UPPER(ep.Diag_cd_123),1,3)||SUBSTR(UPPER(ep.DIAG_CD_4),1,1) epi_diag_1234 ,

				diag.diag_cd_1234, 
				
				phen.CODE, phen.DESC, phen.name
				
			from 
				SAILWWMCCV.WMCC_PEDW_DIAG_20210805 diag
right outer join
				(
				SELECT DISTINCT name, code, DESC FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS 
				UNION 
				SELECT DISTINCT name, code, DESC FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
				) phen
		on
				diag.diag_cd_1234=phen.CODE
				OR 
				DIAG.DIAG_CD_123 =PHEN.CODE

LEFT JOIN
				SAILWWMCCV.WMCC_PEDW_SPELL_20210805 sp
		ON			
				diag.prov_unit_cd=sp.prov_unit_cd
				and
				diag.SPELL_NUM_E=sp.SPELL_NUM_E
				AND 
				sp.alf_sts_cd in ('1','4','39')
left join
				SAILWWMCCV.WMCC_PEDW_EPISODE_20210805 ep
				on 
				sp.prov_unit_cd=ep.prov_unit_cd
				AND 
				sp.SPELL_NUM_E=ep.SPELL_NUM_E
				AND 
				diag.epi_num=ep.epi_num
WHERE SP.ALF_E  IS NOT NULL 
AND 
SP.GNDR_CD IS NOT NULL 
AND 
SP.ADMIS_DT IS NOT NULL
AND 
YEAR(sp.ADMIS_DT) >=2000; 

SELECT COUNT(DISTINCT ALF_E), COUNT(*) FROM sailwWMCCV.ccu002_03_cohort_outcome_pedw;

SELECT 
DISTINCT YEAR(ADMIS_DT), 
DIAG_CD_1234,
CODE, 
DESC,
COUNT(DISTINCT ALF_E)
FROM 
sailwWMCCV.ccu002_03_cohort_outcome_pedw 
GROUP BY 
YEAR(ADMIS_DT),
DIAG_CD_1234,
CODE, 
DESC;

SELECT DISTINCT ALT_CODE , DESCRIPTION FROM SAILUKHDV.ICD10_CODES_AND_TITLES_AND_METADATA icatam 
WHERE ALT_CODE IN (SELECT DISTINCT DIAG_CD_1234 FROM sailwWMCCV.ccu002_03_cohort_outcome_pedw)
ORDER BY ALT_CODE;
