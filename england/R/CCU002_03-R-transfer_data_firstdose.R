# This script transfers data from Databricks
# Author: Venexia Walker
# Date: 2021-09-23

rm(list = ls())

# Setup Databricks connection --------------------------------------------------

con <- DBI::dbConnect(odbc::odbc(),
                      "Databricks",
                      timeout = 60,
                      PWD = rstudioapi::askForPassword("Password please:"))

# Transfer data from DataBricks ------------------------------------------------

chunks <- 7
df <- NULL

for (i in 1:chunks) {
  
  print(paste0("Transferring chunk ",i," of ",chunks,"."))
  tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_03_cohort_firstdose WHERE CHUNK='",i,"'"))
  df <- rbind(df,tmp)
  
}

data.table::fwrite(df,paste0("data/ccu002_03_cohort_firstdose_",gsub("-","",Sys.Date()),".csv.gz"))