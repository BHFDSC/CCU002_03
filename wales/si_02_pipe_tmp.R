## =============================================================================
## Pipeline (2): Reads in analysis-specific data, loads parameters, 
## gets vaccine-specific dataset -- censoring at appropriate dates
##
## Author: Samantha Ip
## =============================================================================
print(rstudioapi::getSourceEditorContext()$path)

ls_events <- c("any_myo_or_pericarditis")


# specify path to data

if (dose_str == "dose2"){
  master_df_fpath <- "P:/torabif/workspace/CCU0002-03/data/ccu002_03_cohort_20210915.qs"
} else if (dose_str == "dose1"){
  master_df_fpath <- "P:/torabif/workspace/CCU0002-03/data/ccu002_03_cohort_firstdose_20210924.qs"
  vaccination_dose2_date_fpath <- "P:/torabif/workspace/CCU0002-03/data/ccu002_03_cohort_20210915.qs"
  }
# old_master_df_fpath <- "~/dars_nic_391419_j3w9t_collab/CCU002_02/data/ccu002_vacc_cohort.csv.gz" # was for region 


# specify study parameters
agebreaks <- c(0, 40, 70, 500)
agelabels <- c("-40", "40-69", "+70") #chage to adopt to windows
noncase_frac <- 0.1


cohort_start_date <- as.Date("2020-12-08")
cohort_end_date   <- as.Date("2021-05-17")

cuts_weeks_since_expo <- c(2, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 
cuts_weeks_since_expo_reduced <- c(4, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 

expo <- ifelse(dose_str == "dose2", "VAC2", "VAC1")




# inspect column names of dataset
master_names <- sort(names(qread(master_df_fpath)))


# old_master_names <- fread(old_master_df_fpath, nrows=1)
# sort(names(old_master_names))

#=========================== READ IN DATA ======================================
# cohort_vac ----

if (dose_str=="dose1"){
  
  cohort_vac <- qread(master_df_fpath) %>% 
            dplyr::select("NHS_NUMBER_DEID", 
             "cov_sex", 
             "out_death", 
             "cov_age", 
             "vaccination_dose1_date", 
             # "vaccination_dose2_date", 
             "vaccination_dose1_product"
            # "cov_region"
    )
  d2_date <- qread(vaccination_dose2_date_fpath)
  library(dplyr)
  d2_date <-  d2_date%>%  dplyr::select(NHS_NUMBER_DEID, vaccination_dose2_date)
  
  cohort_vac <- cohort_vac %>% left_join(d2_date)
  
  setnames(cohort_vac, 
           old = c("cov_sex", 
                   "out_death", 
                   "cov_age", 
                   "vaccination_dose1_date", 
                   "vaccination_dose2_date",
                   "vaccination_dose1_product"
             #      "cov_region"
             ), 
           new = c("SEX", 
                   "DATE_OF_DEATH", 
                   "AGE_AT_COHORT_START", 
                   "VACCINATION_DATE", 
                   "VACCINATION_DATE_SECOND",
                   "VACCINE_PRODUCT"
                   #"region_name"
                   ))
} else if (dose_str=="dose2"){
  cohort_vac <- qread(master_df_fpath) %>% 
                      dplyr::select("NHS_NUMBER_DEID", 
                               "cov_sex", 
                               "out_death", 
                               "cov_age", 
                               "vaccination_dose1_date", 
                               "vaccination_dose2_date", 
                               "vaccination_dose2_product"
  #                             "cov_region"
                      )
  
  setnames(cohort_vac, 
           old = c("NHS_NUMBER_DEID",
                   "cov_sex", 
                   "out_death", 
                   "cov_age", 
                   "vaccination_dose2_date", 
                   "vaccination_dose2_product", 
                   "vaccination_dose1_date"
#                   "cov_region"
                   ), 
           new = c("NHS_NUMBER_DEID",
                   "SEX", 
                   "DATE_OF_DEATH", 
                   "AGE_AT_COHORT_START", 
                   "VACCINATION_DATE", 
                   "VACCINE_PRODUCT", 
                   "START_DATE"
#                   "region_name"
))
}


# # merge region_name from old df ----
# df_region <- fread(old_master_df_fpath, 
#                     select=c("NHS_NUMBER_DEID", 
#                              "region_name"
#                     ))
# cohort_vac <- merge(cohort_vac, df_region, all.x=TRUE)
# any(is.na(cohort_vac$region_name))

print("got cohort_vac with regions ......")

gc()

# covars ----
if (! mdl %in% c("mdl1_unadj", "mdl2_agesex")){
  covar_names <- c(
    "NHS_NUMBER_DEID","cov_age", "cov_sex", "cov_deprivation", "cov_ethnicity", "cov_myo_or_pericarditis", "cov_prior_covid19")
    
  
  covars <- qread(master_df_fpath) %>% 
    dplyr::select(covar_names)
  gc()
  source(file.path(scripts_dir, "si_prep_covariates.R"))
} else {
  covars <- cohort_vac %>% dplyr::select(NHS_NUMBER_DEID)
  print(covars)
}

if ( mdl %in% c("mdl3b_fullyadj")){
  covar_names <- c(
    "NHS_NUMBER_DEID","cov_age", "cov_sex", "cov_deprivation", "cov_ethnicity", "cov_myo_or_pericarditis", "cov_prior_covid19")
  
  
  covars <- qread(master_df_fpath) %>% 
    dplyr::select(covar_names)
  gc()
  source(file.path(scripts_dir, "si_prep_covariates.R"))
} else {
  covars <- cohort_vac %>% dplyr::select(NHS_NUMBER_DEID)
  print(covars)
}

print("got covars......")

#--------------------SET DATES OUTSIDE RANGE AS NA -----------------------------
set_dates_outofrange_na <- function(df, colname)
{
  if (dose_str=="dose1"){df$START_DATE <- cohort_start_date}
  
 
  df <- df %>% mutate(
    !!sym(colname) := as.Date(ifelse((!!sym(colname) > cohort_end_date) | (!!sym(colname) < START_DATE) | (!!sym(colname) < cohort_start_date), 
                                     NA, !!sym(colname) ), origin='1970-01-01')
  )

  return(df)
}


#=======================GET VACCINE-SPECIFIC DATASET============================
get_vac_specific_dataset <- function(survival_data, vac_of_interest){
  survival_data$DATE_VAC_CENSOR <- as.Date(ifelse(!(survival_data$VACCINE_PRODUCT %in% vac_of_interest),
                                                  survival_data$expo_date, 
                                                  NA), origin='1970-01-01')
  
  if (dose_str=="dose1"){
    survival_data <-  transform(survival_data, DATE_VAC_CENSOR = pmin(VACCINATION_DATE_SECOND, DATE_OF_DEATH, DATE_VAC_CENSOR, na.rm=TRUE))
  } 
  
  
  survival_data$expo_date <- as.Date(ifelse((!is.na(survival_data$DATE_VAC_CENSOR)) & (survival_data$expo_date >= survival_data$DATE_VAC_CENSOR), NA, survival_data$expo_date), origin='1970-01-01')
  survival_data$record_date <- as.Date(ifelse((!is.na(survival_data$DATE_VAC_CENSOR)) & (survival_data$record_date >= survival_data$DATE_VAC_CENSOR), NA, survival_data$record_date), origin='1970-01-01')
  
  cat(paste("vac-sepcific df: should see vacs other than", paste(vac_of_interest, collapse = "|"), "as DATE_VAC_CENSOR ... \n", sep="..."))
  print(head(survival_data, 30 ))
  
  cat(paste("min-max expo_date: ", min(survival_data$expo_date, na.rm=TRUE), max(survival_data$expo_date, na.rm=TRUE), "\n", sep="   "))
  cat(paste("min-max record_date: ", min(survival_data$record_date, na.rm=TRUE), max(survival_data$record_date, na.rm=TRUE), "\n", sep="   "))
  
  return(survival_data)
}

print("done 02_pipe......")