## =============================================================================
## MODEL1: UNADJUSTED
## (1) Prep outcome and analysis specific dataset
##
## Author: Samantha Ip
## =============================================================================
source(file.path(scripts_dir,"si_fit_model.R"))
  
  
get_vacc_res <- function(sex_as_interaction, event, vac_str, agegp, cohort_vac, covars){
  outcomes <- qread(master_df_fpath) %>% 
            dplyr::select(
            "NHS_NUMBER_DEID", 
                   "out_any_myo_or_pericarditis" ,
                   "out_death")
  print(c("NHS_NUMBER_DEID", paste0("out_", event)))
  outcomes$name <- event
  setnames(outcomes, 
           old = c(paste0("out_", event)), 
           new = c("record_date"))
  
  if (dose_str=="dose2"){
    cohort_vac <- cohort_vac %>% filter(!is.na(START_DATE))
  }
  
  survival_data <- cohort_vac %>% left_join(outcomes)
  any(survival_data$START_DATE < cohort_start_date)
  any(survival_data$START_DATE > cohort_end_date)
  
  
  schema <- sapply(survival_data, is.Date)
  for (colname in names(schema)[schema==TRUE]){
    print(colname)
    survival_data <- set_dates_outofrange_na(survival_data, colname)
  }
  if (dose_str=="dose2"){
    survival_data <- survival_data %>% filter(!is.na(START_DATE))
  }
  
  cat(paste0("any record_date < START_DATE...... ", 
             any(survival_data$record_date < survival_data$START_DATE, na.rm=TRUE),
             "\n"))
  cat(paste0("range record_dates...... ", paste(range(survival_data$record_date, na.rm=TRUE), collapse=", "), "\n"))
  
  names(survival_data)[names(survival_data) == 'VACCINATION_DATE'] <- 'expo_date'
  
  cat("survival_data before vac specific... \n")
  print(head(survival_data, 20))
  
  if (vac_str=="vac_az"){
    vac_of_interest <- c("COVID-19 (ASTRAZENECA)")
  } else if (vac_str=="vac_pf"){
    vac_of_interest <- c("COVID-19 (PFIZER BIONTECH)")
  } else if (vac_str=="vac_all"){
    vac_of_interest <- unique(na.omit(survival_data$VACCINE_PRODUCT))
  }
  
  survival_data <- get_vac_specific_dataset(survival_data, vac_of_interest)
  

  
  for(fml in c("+ weeks + agegroup + sex")){
    fit_model_reducedcovariates(fml, covars, vac_str, agegp, event, survival_data)
  }

  # fit_model_reducedcovariates(fml="+ weeks + agegroup + sex", covars, vac_str, agegp, event, survival_data)
}



