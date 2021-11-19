SELECT * FROM 
(SELECT 'V1:VACCINATED AT LEAST WITH ONE DOSE' VACC, count(DISTINCT ALF_E ) PATIENT_CNT 	FROM sailwWMCCV.ccu002_03_cohort_analysis
WHERE VACCINATION_DOSE1_DATE IS NOT NULL 
UNION 
SELECT 'V2:VACCINATED WITH BOTH DOSE' VACC, count(DISTINCT ALF_E ) PATIENT_CNT	FROM sailwWMCCV.ccu002_03_cohort_analysis
WHERE VACCINATION_DOSE1_DATE IS NOT NULL 
AND 
vaccination_dose2_date IS NOT NULL
);