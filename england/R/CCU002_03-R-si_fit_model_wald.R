## =============================================================================
## Wald tests for age and sex 
##
## Author: Samantha Ip
## =============================================================================
source(file.path(scripts_dir,"si_fit_get_data_surv.R"))
library(AER)
library(stringi)  
library(stringr) 
library(multcomp)

#------------------------ GET FORMULA  ---------------------------
### interaction----
fmls_wald <- function(interacting_feat, redcovariates_excl_region){
  fml_ref_wald <- paste0(
    "Surv(tstart, tstop, event) ~ week1_2 + week3_23 + ", interacting_feat, "+ week1_2:", interacting_feat, "+ week3_23:", interacting_feat,
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
  cat(paste0("Wald ref...... ", fml_ref_wald, "\n"))
  
  fml_week1_2_wald <- paste0(
    "Surv(tstart, tstop, event) ~ week1_2 + week3_23 + ", interacting_feat, "+ week3_23:", interacting_feat,
    "+", paste(redcovariates_excl_region, collapse="+"),
    "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
  cat(paste0("Wald week1_2...... ", fml_week1_2_wald, "\n"))
  
  fml_week3_23_wald <- paste0(
    "Surv(tstart, tstop, event) ~ week1_2 + week3_23 + ", interacting_feat, "+ week1_2:", interacting_feat,
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
  cat(paste0("Wald week3_23...... ", fml_week3_23_wald, "\n"))
  
  return(list(fml_ref_wald, fml_week1_2_wald, fml_week3_23_wald))
}


# COXFIT ----

fits_and_wald <- function(fml, interval_names, vac_str, data_surv, fixed_covars, covar_names){
  # get Surv formula 
  if (fml == "+ week*agegroup + sex"){
    cat("...... + week*agegroup + sex ...... \n")
    interacting_feat <- "agegroup"
    redcovariates_excl_region <- unique(c("SEX",covar_names, fixed_covars))
    redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("agegroup", interval_names)]
  } else if (fml == "+ week*sex + agegroup"){
    cat("...... + week*sex + agegroup ...... \n")
    interacting_feat <- "SEX"
    redcovariates_excl_region <- unique(c("agegroup",covar_names, fixed_covars))
    redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("SEX", interval_names)]
  }
  
  ls_fml_red <- fmls_wald(interacting_feat, redcovariates_excl_region)
  fml_ref_wald <- ls_fml_red[[1]]
  fml_week1_2_wald <- ls_fml_red[[2]]
  fml_week3_23_wald <- ls_fml_red[[3]]
  
  # fit coxph() ----
  cat(paste0("fitting ...... fml_ref_wald ......\n"))
  system.time(fit_ref_wald <- coxph(
    formula = as.formula(fml_ref_wald), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  
  cat(paste0("fitting ...... fml_week1_2_wald ......\n"))
  system.time(fit_week1_2_wald <- coxph(
    formula = as.formula(fml_week1_2_wald), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  
  cat(paste0("fitting ...... fml_week3_23_wald ......\n"))
  system.time(fit_week3_23_wald <- coxph(
    formula = as.formula(fml_week3_23_wald), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  saveRDS(fit_ref_wald, paste0("fit_ref_wald_", expo, "_", interacting_feat, "_", fml, "_", vac_str,  ".rds"))
  saveRDS(fit_week1_2_wald, paste0("fit_week1_2_wald_",  expo, "_", interacting_feat, "_",fml, "_", vac_str,  ".rds"))
  saveRDS(fit_week3_23_wald, paste0("fit_week3_23_wald_", expo, "_", interacting_feat, "_", fml, "_", vac_str,  ".rds"))
  
  
  # Wald tests ----
  wald_week1_2 <- as.data.frame(broom::tidy(waldtest(fit_ref_wald, fit_week1_2_wald)))
  wald_week3_23 <- as.data.frame(broom::tidy(waldtest(fit_ref_wald, fit_week3_23_wald)))
  wald_week1_2$week <- "week1_2"
  wald_week3_23$week <- "week3_23"
  wald_res <- rbind(wald_week1_2, wald_week3_23)
  
  wald_res$fml <- fml
  wald_res$interacting_feat <- interacting_feat
  wald_res$vac_str <- vac_str
  print(wald_res)

  write.csv(wald_res, paste0("wald_", expo, "_", interacting_feat, "_", fml,  "_", vac_str, ".csv"), row.names = FALSE)
  
  
  gc()
  return(wald_res)
}


# MAIN ----
fit_model_reducedcovariates <- function(fml, covars, vac_str, agegp, event, survival_data){
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(fml, vac_str, covars,  agegp, event, survival_data, cuts_weeks_since_expo)
  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  print(interval_names)

  covar_names <- names(covars)[ names(covars) != "NHS_NUMBER_DEID"]
  
  data_surv <- data_surv %>% left_join(covars)
  data_surv$agegroup <- relevel(factor(data_surv$agegroup), ref = "40to69")
  data_surv <- data_surv %>% mutate(SEX = factor(SEX))
  data_surv <- data_surv %>% mutate(expo = factor(expo))
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)
  
  cat("... data_surv ... \n")
  str(data_surv)
  
  gc()
  
  fixed_covars <- c() # no forced covariates for this study
  
  
  wald_res <- fits_and_wald(fml, interval_names, vac_str, data_surv, fixed_covars, covar_names)
  cat(("... wald_res ... \n"))
  print(wald_res)

  gc()
  
  

  
}

