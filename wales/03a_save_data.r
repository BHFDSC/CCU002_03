# save local copy of the analysis table ========================================

usr <- Sys.info()["user"]

# Fatemeh ======================================================================
setwd("P:/torabif/workspace/CCU0002-03")
   library(RODBC);
    library(data.table)
    source("S:/0000 - Analysts Shared Resources/r-share/login_box.r");
    login = getLogin();
    sql = odbcConnect('PR_SAIL',login[1],login[2]);
    login = 0

    cohort <- sqlQuery(sql,"SELECT * FROM sailwWMCCV.ccu002_03_cohort_analysis;")
    setnames(cohort, tolower(names(cohort[1:ncol(cohort)])))
    qsave(cohort, file = "data/analysis_cohort.qs")

# dose 1 table
    d1 <- sqlQuery(sql,"SELECT * FROM sailwWMCCV.ccu002_03_cohort_analysis_dose1;")
    names(d1)[2:ncol(d1)] <- tolower(names(d1)[2:ncol(d1)])
#    setnames(d1, tolower(names(d1[2:ncol(d1)])))
    qsave(d1, file = "data/ccu002_03_cohort_firstdose_20210924.qs")
    

# dose 2 table    
    d2 <- sqlQuery(sql,"SELECT * FROM sailwWMCCV.ccu002_03_cohort_analysis_dose2;")
    names(d2)[2:ncol(d2)] <- tolower(names(d2)[2:ncol(d2)])
    qsave(d2, file = "data/ccu002_03_cohort_20210915.qs")
    
    