# Sample selection criteria summary ============================================
# how does criteria effect number of individuals eligible for analysis and
# number of events under analysis

    library(RODBC);
    source("S:/0000 - Analysts Shared Resources/r-share/login_box.r");
    login = getLogin();
    sql = odbcConnect('PR_SAIL',login[1],login[2]);
    login = 0
#===============================================================================    
q_summary <- "
SELECT * FROM (
    SELECT
        0                            					  AS step,
       'in c20'						 					  AS criteria,
        COUNT(*)                     					  AS n_alf,
        NULL						 					  AS n_event
    FROM
        SAILWMCCV.C19_COHORT20 cc 
UNION
    SELECT
        1                            					 AS step,
       'age>18 and event anytime post 2000'				 AS criteria,
        COUNT(*)                     					 AS n_alf,
        SUM(out_myo_pericarditis IS NOT null)            AS n_event
    FROM
        SAILWWMCCV.ccu002_03_cohort_base
UNION
    SELECT
        1                            					 AS step,
       'age>18 and event anytime post 2020'				 AS criteria,
        COUNT(*)                     					 AS n_alf,
        SUM(out_myo_pericarditis IS NOT null)            AS n_event
    FROM
        SAILWWMCCV.ccu002_03_cohort_base        
        WHERE out_myo_pericarditis >= '2020-01-01'
UNION
    SELECT
        2                            					 AS step,
        'vaccine records and event post 8th Dec 2020'    AS criteria,
        COUNT(*)                                         AS n_alf,
        SUM(out_any_myo_or_pericarditis IS NOT null)     AS n_event
    FROM
        sailwWMCCV.ccu002_03_cohort_analysis_dose1 
--        WHERE out_any_myo_or_pericarditis >= '2020-12-08'
)ORDER BY step ;
"

library(data.table)
t_summary <-
    sqlQuery(sql, q_summary)
    setnames(t_summary, tolower(names(t_summary[1:ncol(t_summary)])))
t_summary <-
    t_summary  %>%
    arrange(step) %>%
    mutate(
        diff_alf = n_alf - lag(n_alf),
        pdiff_alf = n_alf / first(n_alf) * 100,
        .after = n_alf
    ) %>%
    mutate(
        diff_event = n_event - lag(n_event),
        pdiff_event = n_event / first(n_event) * 100,
        .after = n_event
    )

write_csv(
    t_summary,
    file = "P:/torabif/workspace/CCU0002-03/results/t_sample_selection_summary.csv"
)