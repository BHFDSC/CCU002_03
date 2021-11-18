--------------------------------------------------------------------------------
--Project: 
--BY: fatemeh.torabi@swansea.ac.uk - Stuart Bedeston - Ashley Akbari - Hoda Abassizanjani - Jane Lyons
--DT: 2021-10-06
--aim: create ccu002_03 cohort
--------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS('sailwWMCCV.ccu002_03_cohort_vaccine');

CREATE TABLE sailwWMCCV.ccu002_03_cohort_vaccine (
    alf_e                    BIGINT NOT NULL,
    c20_wob                  DATE,
    c20_gndr_cd              SMALLINT,
    c20_cohort_start_date    DATE,
    c20_move_out_date        DATE,
    c20_cohort_end_date      DATE,
    c20_gp_end_date          DATE,
    c20_gp_coverage_end_date DATE,
    age                      SMALLINT,
    lsoa2011_cd              VARCHAR(10),
    wimd_2019_rank           INTEGER,
    wimd_2019_decile         SMALLINT,
    wimd_2019_quintile       SMALLINT,
    has_death                SMALLINT,
    death_date               DATE,
    
    prior_testing_n          SMALLINT,
    has_positive_test        SMALLINT,
    positive_test_date       DATE,
    
    is_death_covid           SMALLINT,
    has_vacc                 SMALLINT,
    has_bad_vacc_record      SMALLINT,
    vacc_first_date          DATE,
    vacc_first_name          VARCHAR(50),
    vacc_second_date         DATE,
    vacc_second_name         VARCHAR(50),
    is_sample                SMALLINT NOT NULL DEFAULT 0,
--all outcomes
    Myocarditis_dt       Date,
    Myocarditis	         integer,    

    Pericarditis_dt		 Date ,
    Pericarditis 		 integer, 
    PRIMARY KEY (alf_e)
) DISTRIBUTE BY HASH (alf_e);

GRANT ALL ON TABLE sailwWMCCV.ccu002_03_cohort_vaccine TO ROLE NRDASAIL_SAIL_0911_ANALYST;

/* initial insert of c20, mortality and vaccination info */

INSERT INTO sailwWMCCV.ccu002_03_cohort_vaccine (
    alf_e,
    c20_wob,
    c20_gndr_cd,
    c20_cohort_start_date,
    c20_move_out_date,
    c20_cohort_end_date,
    c20_gp_end_date,
    c20_gp_coverage_end_date,
    age,
    lsoa2011_cd,
    wimd_2019_rank,
    wimd_2019_decile,
    wimd_2019_quintile,
    has_death,
    death_date,
    is_death_covid,
    prior_testing_n,
    has_positive_test,
    positive_test_date,    
    has_vacc,
    has_bad_vacc_record,
    vacc_first_date,
    vacc_first_name,
    vacc_second_date,
    vacc_second_name
)
WITH
    /* event: any vaccinated */
    alf_vacc_flg AS (
        SELECT
            vacc.alf_e,
            MAX(vacc.alf_has_bad_vacc_record) AS alf_has_bad_vacc_record
        FROM
            sailWMCCV.C19_COHORT20_RRDA_CVVD_20210921 AS vacc
        GROUP BY
        	vacc.alf_e
    ),
    /* event: first vaccination */
    alf_vacc_first AS (
        SELECT
            vacc.alf_e,
            vacc.vacc_date,
            vacc.vacc_dose_num,
            vacc.vacc_name
        FROM
            sailWMCCV.C19_COHORT20_RRDA_CVVD_20210921 AS vacc
        WHERE
            alf_has_bad_vacc_record = 0 AND
            vacc_dose_num           = 1
    ),
    /* event: second vaccination */
    alf_vacc_second AS (
        SELECT
            vacc.alf_e,
            vacc.vacc_date,
            vacc.vacc_dose_num,
            vacc.vacc_name
        FROM
            sailWMCCV.C19_COHORT20_RRDA_CVVD_20210921 AS vacc
        WHERE
            alf_has_bad_vacc_record = 0 AND
            vacc_dose_num           = 2
    ),
   /* summary: number of tests prior to cohort start date */
    alf_test_summary AS (
        SELECT
            test.alf_e,
            COUNT(distinct spcm_collected_dt) AS n
        FROM
        	sailWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS_20210921  AS test
        WHERE
        	/* this the same number of days (282) as the Scottish work, they
        	 * went with 2020-03-01 <= x < 2020-12-08
        	 */
	        test.spcm_collected_dt >= '2020-01-01' AND
            test.spcm_collected_dt <  '2020-12-07' AND
            test.alf_e IS NOT NULL
        GROUP BY
            test.alf_e
    ),
    /* event: positive covid test */
    alf_positive_test AS (
        SELECT
            test.alf_e,
            MIN(test.spcm_collected_dt) AS positive_test_date
        FROM
        	sailWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS_20210921  AS test
        WHERE
            test.covid19testresult = 'Positive' AND
            test.alf_e IS NOT NULL
        GROUP BY
            test.alf_e
    ),    
    /* event: covid and non-covid deaths */
    alf_death AS (
        SELECT
            mortality.alf_e,
            mortality.dod,
            CASE
                WHEN mortality.covid_yn_underlying              = 'y' THEN 1
                WHEN mortality.covid_yn_secondary               = 'y' THEN 1
                WHEN mortality.covid_yn_underlying_or_secondary = 'y' THEN 1
                WHEN mortality.cdds_positive_covid_19_flg       =  1  THEN 1
                ELSE 0
            END AS is_death_covid,
            ROW_NUMBER() OVER (PARTITION BY mortality.alf_e ORDER BY mortality.dod) AS seq
        FROM
            sailwmc_v.c19_cohort20_mortality AS mortality
        WHERE
            mortality.dod   IS NOT NULL AND
            mortality.alf_e IS NOT NULL
    ),
    alf_death_dedup AS (
        SELECT
            alf_e,
            dod AS death_date,
            is_death_covid
        FROM
            alf_death
        WHERE
            seq = 1
    ),
    /* WDSD clean table for ALFs at start of vaccination programme */
    alf_area AS (
	SELECT
		*
	FROM
		sailWMCCV.C19_COHORT_WDSD_CLEAN_ADD_GEOG_CHAR_LSOA2011_20210921 
	WHERE
        start_date <= '2020-12-07' AND
        end_date   >= '2020-12-07'
	)
SELECT
    /* c19 cohort2020 */
    c20.alf_e,
    c20.wob                                              AS c20_wob,
    c20.gndr_cd                                          AS c20_gndr_cd,
    c20.cohort_start_date                                AS c20_cohort_start_date,
    c20.move_out_date                                    AS c20_move_out_date,
    c20.cohort_end_date                                  AS c20_cohort_end_date,
    c20.gp_end_date                                      AS c20_gp_end_date,
    c20.gp_coverage_end_date                             AS c20_gp_coverage_end_date ,
    /* calculate age */
    FLOOR((DAYS('2021-02-28') - DAYS(c20.wob)) / 365.25) AS age,
    /* area at 7th December */
    alf_area.lsoa2011_cd                                 AS lsoa2011_cd,
    alf_area.wimd_2019_rank                              AS wimd_2019_rank,
    alf_area.wimd_2019_decile                            AS wimd_2019_decile,
    alf_area.wimd_2019_quintile                          AS WIMD_2019_QUINTILE,
    /* death */
    CAST(
        alf_death_dedup.alf_e IS NOT NULL AS INTEGER
    )                                                    AS has_death,
    alf_death_dedup.death_date,
    alf_death_dedup.is_death_covid,
    /* prior testing */
    CASE
        WHEN alf_test_summary.n IS NULL THEN 0
        ELSE alf_test_summary.n
    END 												AS prior_testing_n,
    /* positive test */
    CAST(
        alf_positive_test.alf_e IS NOT NULL AS INTEGER
    )                                                    AS has_positive_test,
    alf_positive_test.positive_test_date,    
    /* vaccination */
    CAST(
        alf_vacc_flg.alf_e IS NOT NULL AS INTEGER
    )                                                    AS has_vacc,
    alf_vacc_flg.alf_has_bad_vacc_record                 AS has_bad_vacc_record,
    alf_vacc_first.vacc_date                             AS vacc_first_date,
    alf_vacc_first.vacc_name                             AS vacc_first_name,
    alf_vacc_second.vacc_date                            AS vacc_second_date,
    alf_vacc_second.vacc_name                            AS vacc_second_name
FROM
    sailWMCCV.C19_COHORT20_20210921 AS c20
LEFT JOIN
    alf_death_dedup
    ON c20.alf_e = alf_death_dedup.alf_e
LEFT JOIN
    alf_test_summary
    ON c20.alf_e = alf_test_summary.alf_e
LEFT JOIN
    alf_positive_test
    ON c20.alf_e = alf_positive_test.alf_e    
LEFT JOIN
    alf_vacc_flg
    ON c20.alf_e = alf_vacc_flg.alf_e
LEFT JOIN
    alf_vacc_first
    ON c20.alf_e = alf_vacc_first.alf_e
LEFT JOIN
    alf_vacc_second
    ON c20.alf_e = alf_vacc_second.alf_e
LEFT JOIN
    alf_area
    ON c20.alf_e = alf_area.alf_e;

 --check
SELECT * FROM sailwWMCCV.ccu002_03_cohort_vaccine;
SELECT count(DISTINCT alf_e) FROM sailwWMCCV.ccu002_03_cohort_vaccine;


/* who is eligible to be in the cohort?
 *
 * criteria are:
 *  - in c20
 *  - in c20 at cohort start
 *  - has wob and sex
 *  - has lsoa at cohort start
 *  - is registered with a GP at cohort start
 *  - has no bad vacc records
 *  - has qcovid measures
 *  - aged 15 years or older
 */

/* update is_sample in cohort table with those that meet the criteria */

UPDATE sailwWMCCV.ccu002_03_cohort_vaccine 
SET is_sample = 1
    WHERE
        c20_cohort_end_date > '2020-12-07'
        AND c20_wob IS NOT NULL
        AND c20_gndr_cd IS NOT NULL
        AND lsoa2011_cd IS NOT NULL
        AND c20_gp_end_date > '2020-12-07'
        AND (has_bad_vacc_record IS NULL OR has_bad_vacc_record = 0)
        AND age >= 18
;

COMMIT;
