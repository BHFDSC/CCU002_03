## =============================================================================
## MODEL1: UNADJUSTED
##
## Author: Samantha Ip
## =============================================================================
source(file.path(scripts_dir,"si_fit_get_data_surv.R"))

#install.packages("multcomp")
#library(multcomp)
print("get model formula")
#------------------------ GET FORMULA  ---------------------------
### interaction----
fml_ind_expo_agegroup <- function(covar_names, interval_names, fixed_covars){
  cat("...... + ind_expo*agegroup + sex ...... \n")
  # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
  AMI_bkwdselected_covars <- covar_names
  # interval_names_withpre <- c("week_pre", interval_names)
  redcovariates_excl_region <- unique(c("SEX",AMI_bkwdselected_covars, fixed_covars))
  redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("agegroup", interval_names)]
  # ind_postexpo <- paste0("week1_", cuts_weeks_since_expo[length(cuts_weeks_since_expo)])
  # data_surv$week_pre <- 1*((data_surv[ind_postexpo]==0) )
  # data_surv <- one_hot_encode("SEX", data_surv, interval_names_withpre)
  # print(data_surv)
  
  fml_red <- paste0(
    "Surv(tstart, tstop, event) ~ expo*agegroup",
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID) ") 
  return(fml_red)}


fml_week_agegroup <- function(covar_names, interval_names, fixed_covars){
  cat("...... + week*agegroup + sex ...... \n")
  # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
  AMI_bkwdselected_covars <- covar_names
  redcovariates_excl_region <- unique(c("SEX",AMI_bkwdselected_covars, fixed_covars))
  redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("agegroup", interval_names)]
  
  fml_red <- paste0(
    "Surv(tstart, tstop, event) ~ week*agegroup",
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID) ") 
  return(fml_red)}



fml_ind_expo_sex <- function(covar_names, interval_names, fixed_covars){
  cat("...... + ind_expo*sex + agegroup ...... \n")
  # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
  AMI_bkwdselected_covars <- covar_names
  # interval_names_withpre <- c("week_pre", interval_names)
  redcovariates_excl_region <- unique(c("agegroup", AMI_bkwdselected_covars, fixed_covars))
  redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("SEX", interval_names)]
  # ind_postexpo <- paste0("week1_", cuts_weeks_since_expo[length(cuts_weeks_since_expo)])
  # data_surv$week_pre <- 1*((data_surv[ind_postexpo]==0) )
  # data_surv <- one_hot_encode("SEX", data_surv, interval_names_withpre)
  # print(data_surv)
  
  fml_red <- paste0(
    "Surv(tstart, tstop, event) ~ expo*SEX",
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID)") 
  return(fml_red)}

fml_week_sex <- function(covar_names, interval_names, fixed_covars){
  cat("...... + week*sex + agegroup ...... \n")
  # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
  AMI_bkwdselected_covars <- covar_names
  redcovariates_excl_region <- unique(c("agegroup",AMI_bkwdselected_covars, fixed_covars))
  redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("SEX", interval_names)]
  
  fml_red <- paste0(
    "Surv(tstart, tstop, event) ~ week*SEX",
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID)") 
  return(fml_red)}

### no-interaction----
fml_weeks_agegroup_sex <- function(covar_names, interval_names, fixed_covars){
  cat("...... + weeks + agegroup + sex ...... \n")
  if (mdl == "mdl1_unadj"){
    redcovariates_excl_region <- unique(c(interval_names))
    cat("...... redcovariates_excl_region ...... \n")
    print(unlist(redcovariates_excl_region))
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ ",
      paste(redcovariates_excl_region, collapse="+"), 
      "+ cluster(NHS_NUMBER_DEID) ")
    
    
  } else {
    AMI_bkwdselected_covars <- covar_names
    redcovariates_excl_region <- unique(c("agegroup", interval_names, AMI_bkwdselected_covars, fixed_covars))
    print(unlist(redcovariates_excl_region))
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ ",
      paste(redcovariates_excl_region, collapse="+"), 
      "+ cluster(NHS_NUMBER_DEID) + SEX")
  }
  return(fml_red)}



# COXFIT ----

coxfit_bkwdselection <- function(fml, data_surv, sex, interval_names, fixed_covars, event, agegp, covar_names){
  cat("...... sex ", sex, " ...... \n")
  # which covariates are backward-selected -- to include? 
  # AMI_bkwdselected_covars <- readRDS(file=paste0("backwards_names_kept_vac_all_AMI_", agegp, "_sex", sex,".rds"))
  
  # get Surv formula 
  if (fml == "+ weeks + agegroup + sex"){
    fml_red <- fml_weeks_agegroup_sex(covar_names, interval_names, fixed_covars)
  } else if (fml == "+ ind_expo*sex + agegroup"){
    fml_red <- fml_ind_expo_sex(covar_names, interval_names, fixed_covars)
  } else if (fml == "+ ind_expo*agegroup + sex"){
    fml_red <- fml_ind_expo_agegroup(covar_names, interval_names, fixed_covars)
  } else if (fml == "+ week*sex + agegroup"){
    interval_names_withpre <- c("week_pre", interval_names)
    data_surv$week_pre <- 1*((data_surv$week1==0) & (data_surv$week2==0) & (data_surv$week3_23==0) )
    data_surv <- one_hot_encode(interacting_feat="SEX", data_surv, interval_names_withpre)
    print(data_surv)
    fml_red <- fml_week_sex(covar_names, interval_names, fixed_covars)
  } else if (fml == "+ week*agegroup + sex"){
    interval_names_withpre <- c("week_pre", interval_names)
    data_surv$week_pre <- 1*((data_surv$week1==0) & (data_surv$week2==0) & (data_surv$week3_23==0) )
    data_surv <- one_hot_encode(interacting_feat="agegroup", data_surv, interval_names_withpre)
    print(data_surv)
    fml_red <- fml_week_agegroup(covar_names, interval_names, fixed_covars)
  }
  
  print(fml_red)
  
  # fit coxph() 
  system.time(fit_red <- coxph(
    formula = as.formula(fml_red), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  # if (sex_as_interaction){
  #   cat("save fit_red where sex_as_interaction ...... \n")
  #   saveRDS(fit_red, paste0("fit_", event, "_", agegp, "_", vac_str,  ".rds"))
  # }
  
  
  # presentation 
  fit_tidy <-tidy(fit_red, exponentiate = TRUE, conf.int=TRUE)
  # fit_tidy$sex<-c(sex)
  
  gc()
  print(fit_tidy)
  return(fit_tidy)
}


# MAIN ----
fit_model_reducedcovariates <- function(fml, covars, vac_str, agegp, event, survival_data){
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(fml, vac_str, covars,  agegp, event, survival_data, cuts_weeks_since_expo)
  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  print(interval_names)
  # ind_any_zeroeventperiod <- list_data_surv_noncase_ids_interval_names[[4]]
  # 
  # if (ind_any_zeroeventperiod){
  #   cat("...... COLLAPSING POST-EXPO INTERVALS ......")
  #   list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars,  agegp, event, survival_data, cuts_weeks_since_expo_reduced)
  #   data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  #   noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  #   interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  #   ind_any_zeroeventperiod <- list_data_surv_noncase_ids_interval_names[[4]]
  # }
  
  covar_names <- names(covars)[ names(covars) != "NHS_NUMBER_DEID"]
  
  data_surv <- data_surv %>% left_join(covars)
  data_surv$agegroup <- relevel(factor(data_surv$agegroup), ref = "40-69")
  data_surv <- data_surv %>% mutate(SEX = factor(SEX))
  data_surv <- data_surv %>% mutate(expo = factor(expo))
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)
  
  cat("... data_surv ... \n")
  str(data_surv)
  
  gc()
  
  # saveRDS(data_surv, "data_surv_myoperi_d2az_allagesex_expandedweeks.rds")
  # ------------------- FIXED COVARIATES POST_BACKWARDS-SELECTION --------------
  # for vaccine project these are arterial/venous specific
  # fixed_covars <- c("ANTICOAG_MEDS", "ANTIPLATLET_MEDS", "BP_LOWER_MEDS", "CATEGORISED_ETHNICITY", "EVER_ALL_STROKE",
  #                     "EVER_AMI", "EVER_DIAB_DIAG_OR_MEDS", "IMD", "LIPID_LOWER_MEDS", "smoking_status") 
  # # "ANTIPLATELET_MEDS"
  # 
  # umbrella_venous <- c("DVT_summ_event","PE", 
  #                      "portal_vein_thrombosis",  "ICVT_summ_event", "other_DVT", "Venous_event")
  # 
  # 
  # if (event %in% umbrella_venous){
  #   fixed_covars <- c("CATEGORISED_ETHNICITY", "IMD", "ANTICOAG_MEDS", "COCP_MEDS", "HRT_MEDS", "EVER_PE_VT",  "COVID_infection")
  #   cat("...... VENOUS EVENT FIXED_COVARS! ...... \n")
  # }
  fixed_covars <- c() # hack for skipping backward-selection
  
  
  fit_sexall <- coxfit_bkwdselection(fml, data_surv, sex="all", interval_names, fixed_covars,  event, agegp, covar_names)
  # fit_sex1 <- coxfit_bkwdselection(data_surv %>% filter(SEX==1), "1", interval_names,  fixed_covars, event, agegp, sex_as_interaction, covar_names)
  # fit_sex2 <- coxfit_bkwdselection(data_surv %>% filter(SEX==2), "2", interval_names,  fixed_covars, event, agegp, sex_as_interaction, covar_names)
  # fit <- rbindlist(list(fit_sexall, fit_sex1, fit_sex2))
  fit <- fit_sexall
  fit$event <- event
  fit$agegp <- agegp
  fit$fml <- fml #"week_agegp_sex"
  cat("... fit ... \n")
  print(fit)
  
  write.csv(fit, paste0("P:/torabif/workspace/CCU0002-03/results/tbl_hr_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = F)
  gc()
  
  

  
}



mk_factor_orderlevels <- function(df, colname)
{
  df <- df %>% dplyr::mutate(
    !!sym(colname) := factor(!!sym(colname), levels = str_sort(unique(df[[colname]]), numeric = TRUE)))
  return(df)
}



one_hot_encode <- function(interacting_feat, data_surv, interval_names_withpre){
  cat("...... one_hot_encode ...... \n")
  data_surv <- as.data.frame(data_surv)
  data_surv$week <- apply(data_surv[unlist(interval_names_withpre)], 1, function(x) names( x[x==1]) )
  data_surv$week <- relevel(as.factor( data_surv$week) , ref="week_pre")
  
  data_surv$tmp <- as.factor(paste(data_surv$week, data_surv[[interacting_feat]], sep="_"))
  df_tmp <- as.data.frame(model.matrix( ~ 0 + tmp, data = data_surv))
  names(df_tmp) <- substring(names(df_tmp), 4)
  
  for (colname in names(df_tmp)){
    print(colname)
    df_tmp <- mk_factor_orderlevels(df_tmp, colname)
  }
  
  data_surv <- cbind(data_surv, df_tmp)
  
  str(data_surv)
  return(data_surv)
}


