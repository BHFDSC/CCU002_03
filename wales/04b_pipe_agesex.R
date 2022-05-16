## =============================================================================
## Pipeline (1): Control center, calls relevant analysis scripts, sets working 
## and saving directories, parallelises processes
##
## Author: Samantha Ip
## =============================================================================

library(data.table)
library(dplyr)
library(survival)
library(table1)
library(broom)
library(DBI)
library(ggplot2)
library(nlme)
library(tidyverse)
library(R.utils)
library(lubridate)
library(purrr)
library(qs)
library(parallel)
library(multcomp)
options(error=recover)


#detectCores()


# con <- dbConnect(odbc::odbc(), "Databricks", timeout=60, PWD=rstudioapi::askForPassword("enter databricks personal access token:"))
rm(list=setdiff(ls(), c("con")))
gc()
setwd("P:/torabif/workspace/CCU0002-03")
res_dir_proj <- "P:/torabif/workspace/CCU0002-03/res_myocarditis"
res_dir_date <- "2021-10-07"
# specify path to scripts' directory
scripts_dir <- "P:/torabif/workspace/CCU0002-03"
dose_str <- "dose2"

mdl <- "mdl2_agesex"  #, "mdl3a_bkwdselect", "mdl3b_fullyadj", "mdl4_fullinteract_suppl34", "mdl5_anydiag_death28days", "mdl4_fullinteract_suppl34"


# specify model parameters, data source, outcomes of interest, read in relevant data, get prepped covariates
source(file.path(scripts_dir, 
                 paste0("si_02_pipe_tmp.R")
                 ))

gc()
# -----------SET MODEL-SPECIFIC RESULTS DIR  & CALLS MODEL SCRIPT --------------
if (mdl == "mdl1_unadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "unadj_nosexforcombined")
} else if (mdl == "mdl2_agesex"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "adj_age_sex_only")
# } else if (mdl == "mdl3a_bkwdselect"){
#   res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "fully_adj_bkwdselect")
} else if (mdl == "mdl3b_fullyadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "fully_adj_bkwdselect")
} 


source(file.path(scripts_dir,"si_call_mdl.R"))
# creates if does not exist and sets working directory
# dir.create(res_dir, recursive = TRUE)
#setwd(file.path(res_dir))



# ------------- DETERMINE WHICH COMBOS HAVE NOT BEEN COMPLETED -----------------
if (mdl == "mdl4_fullinteract_suppl34"){
  ls_interacting_feats <- c("age_deci", "SEX", "CATEGORISED_ETHNICITY", "IMD", 
                            "EVER_TCP", "EVER_THROMBOPHILIA", "EVER_PE_VT", "COVID_infection", 
                            "COCP_MEDS", "HRT_MEDS", "ANTICOAG_MEDS", "ANTIPLATLET_MEDS", 
                            "prior_ami_stroke", "EVER_DIAB_DIAG_OR_MEDS")
  
  interactingfeat_age_vac_combos <- expand.grid(ls_interacting_feats, c("Venous_event", "Arterial_event"))
  names(interactingfeat_age_vac_combos) <- c("interacting_feat", "event", "vac")
  ls_should_have <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
                         function(interacting_feat, event, vac) 
                           file.path(res_dir,
                                     "wald_",
                                     interacting_feat, "_",
                                     event, ".csv"
                           ))
  
  ls_should_have <- unlist(ls_should_have)
  
  ls_events_missing <- data.frame()
  
  for (i in 1:nrow(interactingfeat_age_vac_combos)) {
    row <- interactingfeat_age_vac_combos[i,]
    fpath <- file.path(res_dir,
                       "wald_",
                       row$interacting_feat, "_",
                       row$event, ".csv")
    
    if (!file.exists(fpath)) {
      ls_events_missing <- rbind(ls_events_missing, row)
    }
  }
} else {
  # outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("vac_az", "vac_pf"))
  outcome_age_vac_combos <- expand.grid(ls_events, c("all"), c("vac_az", "vac_pf"))
  names(outcome_age_vac_combos) <- c("event", "agegp", "vac")
  ls_should_have <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$vac), 
                         function(event, agegp, vac) 
                           file.path(res_dir,
                                     paste0("tbl_hr_" , expo, "_", event, "_", agegp, "_", vac, ".csv")
                                     ))
  
  ls_should_have <- unlist(ls_should_have)
  
  ls_events_missing <- data.frame()
  
  for (i in 1:nrow(outcome_age_vac_combos)) {
    row <- outcome_age_vac_combos[i,]
    fpath <-file.path(res_dir,
                      paste0("tbl_hr_" , expo, "_", row$event, "_", row$agegp, "_", row$vac, ".csv")
                      )
    
    
    if (!file.exists(fpath)) {
      ls_events_missing <- rbind(ls_events_missing, row)
    }
  }
}



ls_events_missing 
# ls_events_missing <- ls_events_missing %>% filter(! event %in% c("death"))





# ------------------------------------ LAUNCH JOBS -----------------------------
#age sex adjusted: 

lapply(split(ls_events_missing,seq(nrow(ls_events_missing))),
function(ls_events_missing) 
  get_vacc_res(
    sex_as_interaction=FALSE,
    event=ls_events_missing$event,
    vac_str=ls_events_missing$vac,
    agegp=ls_events_missing$agegp, 
    cohort_vac, covars)
)