## =============================================================================
## MODELS -- All models 
##
## Author: Samantha Ip
## =============================================================================
library(multcomp)
source(file.path(scripts_dir,"si_fit_get_data_surv.R"))

#------------------------ GET FORMULA  ---------------------------
### interaction----
fml_week_agegroup <- function(covar_names, interval_names, fixed_covars){
  cat("...... + week*agegroup + sex ...... \n")
  redcovariates_excl_region <- unique(c("SEX",covar_names, fixed_covars))
  redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("agegroup", interval_names)]
  
  fml_red <- paste0(
    "Surv(tstart, tstop, event) ~ week*agegroup",
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID) + region_name") 
  return(fml_red)}

fml_week_sex <- function(covar_names, interval_names, fixed_covars){
  cat("...... + week*sex + agegroup ...... \n")
  redcovariates_excl_region <- unique(c("agegroup",covar_names, fixed_covars))
  redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("SEX", interval_names)]
  
  fml_red <- paste0(
    "Surv(tstart, tstop, event) ~ week*SEX",
    "+", paste(redcovariates_excl_region, collapse="+"), 
    "+ cluster(NHS_NUMBER_DEID) + region_name") 
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
    redcovariates_excl_region <- unique(c("agegroup", interval_names, covar_names, fixed_covars))
    print(unlist(redcovariates_excl_region))
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ ",
      paste(redcovariates_excl_region, collapse="+"), 
      "+ cluster(NHS_NUMBER_DEID) + region_name + SEX")
  }
  return(fml_red)}



# COXFIT ----

coxfit_bkwdselection <- function(fml, vac_str, data_surv, sex, interval_names, fixed_covars, event, agegp, covar_names){
  cat("...... sex ", sex, " ...... \n")

  # get Surv formula 
  if (fml == "+ weeks + agegroup + sex"){
    fml_red <- fml_weeks_agegroup_sex(covar_names, interval_names, fixed_covars)
  } else if (fml == "+ week*sex + agegroup"){
    interval_names_withpre <- c("week_pre", interval_names)
    data_surv$week_pre <- 1*((data_surv$week1_2==0) & (data_surv$week3_23==0) )
    data_surv <- one_hot_encode(interacting_feat="SEX", data_surv, interval_names_withpre)
    print(data_surv)
    fml_red <- fml_week_sex(covar_names, interval_names, fixed_covars)
  } else if (fml == "+ week*agegroup + sex"){
    interval_names_withpre <- c("week_pre", interval_names)
    data_surv$week_pre <- 1*((data_surv$week1_2==0) & (data_surv$week3_23==0) )
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
  
  # save glht ----
  na_coefs <- which(is.na(coefficients(fit_red))) %>% unname()
  if (length(na_coefs)>0){  
    M <- model.matrix(fit_red)[,-na_coefs]
    data_surv_reduced <- cbind(data_surv %>% dplyr::select(tstart, tstop, event) , M)
    system.time(fit_red <- coxph(
      formula = as.formula(paste0("Surv(tstart, tstop, event) ~ `",  paste(colnames(M), collapse="` + `"), "`")), 
      data = data_surv_reduced, weights=data_surv_reduced$cox_weights
    ))
  }
  
  if (fml == "+ week*agegroup + sex"){
    cat("...... glht @ + week*agegroup + sex ...... \n")
    A <- grep("^weekweek1_2$", names(coef(fit_red)))
    B <- grep("weekweek1_2:agegroup0to40", names(coef(fit_red)))
    C <- grep("weekweek1_2:agegroup70to500", names(coef(fit_red)))
    mat_linfct <- matrix(0, 6, length(coef(fit_red)))
    colnames(mat_linfct) <- names(coef(fit_red))
    mat_linfct[1,A] <- 1
    mat_linfct[2,c(A, B)] <- 1
    mat_linfct[3,c(A, C)] <- 1
    mat_linfct[4,A+1] <- 1
    mat_linfct[5,c(A+1, B+1)] <- 1
    mat_linfct[6,c(A+1, C+1)] <- 1
    fit_glht <- as.data.frame(broom::tidy(glht(fit_red, linfct = mat_linfct)))
    fit_glht$contrast <- c("week1_2", "week1_2 + week1_2:agegroup0to40", "week1_2 + week1_2:agegroup70to500",
                           "week3_23", "week3_23 + week3_23:agegroup0to40", "week3_23 + week3_23:agegroup70to500")
    write.csv(fit_glht, paste0("tbl_hr_glht_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = F)
    
                          
  } else if (fml == "+ week*sex + agegroup"){
    cat("...... glht @ + week*sex + agegroup ...... \n")
    A <- grep("^weekweek1_2$", names(coef(fit_red)))
    B <- grep("weekweek1_2:SEX2", names(coef(fit_red)))
    mat_linfct <- matrix(0, 4, length(coef(fit_red)))
    colnames(mat_linfct) <- names(coef(fit_red))
    mat_linfct[1,A] <- 1
    mat_linfct[2,c(A, B)] <- 1
    mat_linfct[3,A+1] <- 1
    mat_linfct[4,c(A+1, B+1)] <- 1
    fit_glht <- as.data.frame(broom::tidy(glht(fit_red, linfct = mat_linfct)))
    fit_glht$contrast <- c("week1_2", "week1_2 + week1_2:SEX2",
                           "week3_23", "week3_23 + week3_23:SEX2")

    # fit_glht <- as.data.frame(broom::tidy(glht(fit_red, linfct = c(
    #   "weekweek1_2 = 0", "weekweek1_2 + weekweek1_2:SEX2 = 0",  
    #   "weekweek3_23 = 0", "weekweek3_23 + weekweek3_23:SEX2 = 0"
    # ))))
    write.csv(fit_glht, paste0("tbl_hr_glht_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = F)
    
  
    }
  
  
  # tidy for presentation ----
  fit_tidy <-tidy(fit_red, exponentiate = TRUE, conf.int=TRUE)
  
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

  
  fit <- coxfit_bkwdselection(fml, vac_str, data_surv, sex="all", interval_names, fixed_covars,  event, agegp, covar_names)
  fit$event <- event
  fit$agegp <- agegp
  fit$fml <- fml #e.g. "week_agegp_sex"
  cat("... fit ... \n")
  print(fit)
  
  write.csv(fit, paste0("tbl_hr_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = F)
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


