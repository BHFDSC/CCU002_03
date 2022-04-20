## =============================================================================
## Pipeline (2): Reads in analysis-specific data, loads parameters, 
## gets vaccine-specific dataset -- censoring at appropriate dates
##
## Author: Samantha Ip
## =============================================================================
print(rstudioapi::getSourceEditorContext()$path)

ls_events <- c("myocarditis")


# specify path to data
master_df_fpath <- "~/dars_nic_391419_j3w9t_collab/CCU002_03/data/ccu002_03_cohort_20220126.csv.gz"
master_names <- fread(master_df_fpath, nrows=1)
sort(names(master_names))

# specify study parameters
agebreaks <- c(0, 40, 70, 500)
# agelabels <- c("<40", "40-69", ">=70")
agelabels <- c("0to40", "40to69", "70to500")

noncase_frac <- 0.1


cohort_start_date <- as.Date("2020-12-08")
cohort_end_date <- as.Date("2021-05-17")

cuts_weeks_since_expo <- c(2, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7)))
# cuts_weeks_since_expo <- c(as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) # reviewer's request

cuts_weeks_since_expo_reduced <- c(4, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 

expo <- ifelse(dose_str == "dose2", "VAC2", "VAC1")


#=========================== READ IN DATA ======================================
# cohort_vac ----
if (dose_str=="dose1"){
  
  cohort_vac <- fread(master_df_fpath, 
                    select=c("NHS_NUMBER_DEID", 
                             "cov_dose1_sex", 
                             "out_death", 
                             "cov_dose1_age", 
                             "vaccination_dose1_date", 
                             "vaccination_dose2_date",
                             "vaccination_dose1_product",
                             "cov_dose1_region"
                    ))

  
  setnames(cohort_vac, 
           old = c("cov_dose1_sex", 
                   "out_death", 
                   "cov_dose1_age", 
                   "vaccination_dose1_date", 
                   "vaccination_dose2_date",
                   "vaccination_dose1_product",
                   "cov_dose1_region"), 
           new = c("SEX", 
                   "DATE_OF_DEATH", 
                   "AGE_AT_COHORT_START", 
                   "VACCINATION_DATE", 
                   "VACCINATION_DATE_SECOND",
                   "VACCINE_PRODUCT",
                   "region_name"))
} else if (dose_str=="dose2"){
  cohort_vac <- fread(master_df_fpath, 
                      select=c("NHS_NUMBER_DEID", 
                               "cov_dose2_sex", 
                               "out_death", 
                               "cov_dose2_age", 
                               "vaccination_dose1_date", 
                               "vaccination_dose2_date", 
                               "vaccination_dose1_product",
                               "vaccination_dose2_product",
                               "cov_dose2_region"
                      ))
  
  setnames(cohort_vac, 
           old = c("cov_dose2_sex", 
                   "out_death", 
                   "cov_dose2_age", 
                   "vaccination_dose2_date", 
                   "vaccination_dose2_product", 
                   "vaccination_dose1_date",
                   "vaccination_dose1_product",
                   "cov_dose2_region"), 
           new = c("SEX", 
                   "DATE_OF_DEATH", 
                   "AGE_AT_COHORT_START", 
                   "VACCINATION_DATE", 
                   "VACCINE_PRODUCT", 
                   "START_DATE",
                   "D1_PRODUCT",
                   "region_name"))
}

print("got cohort_vac with regions ......")

gc()

# covars ----
if (! mdl %in% c("mdl1_unadj", "mdl2_agesex")){
  covar_names <- master_names %>% dplyr::select(
    "NHS_NUMBER_DEID",
    setdiff(names(master_names)[grepl(paste0("^cov_", dose_str), names(master_names))], 
            c(paste0("cov_", dose_str, "_age"), 
              paste0("cov_", dose_str, "_sex"),
              paste0("cov_", dose_str, "_region"),
              paste0("cov_", dose_str, "_lsoa"))
    )) %>% names()
  
  covars <- fread(master_df_fpath, select = covar_names)
  gc()
  names(covars) <- sub(paste0(dose_str, "_"), "", names(covars))
  
  source(file.path(scripts_dir, "si_prep_covariates.R"))
} else {
  covars <- cohort_vac %>% dplyr::select(NHS_NUMBER_DEID)
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

  
  if (dose_str=="dose1"){
    survival_data$DATE_VAC_CENSOR <- as.Date(ifelse(!(survival_data$VACCINE_PRODUCT %in% vac_of_interest),
                                                    survival_data$expo_date, 
                                                    NA), origin='1970-01-01')
    survival_data <-  transform(survival_data, DATE_VAC_CENSOR = pmin(VACCINATION_DATE_SECOND, DATE_VAC_CENSOR, na.rm=TRUE))
  } else if (dose_str=="dose2"){
    survival_data <- survival_data %>% dplyr::filter(survival_data$D1_PRODUCT %in% vac_of_interest)
    survival_data$DATE_VAC_CENSOR <- as.Date(ifelse((!(survival_data$VACCINE_PRODUCT %in% vac_of_interest)),
                                                    survival_data$expo_date, 
                                                    NA), origin='1970-01-01')
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
