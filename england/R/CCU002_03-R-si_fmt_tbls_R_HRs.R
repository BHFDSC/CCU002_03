## =============================================================================
## FORMATS RESULTS from RSTUDIO
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
library(parallel)

detectCores()
rm(list=ls())
#========================== INITIALIZE/PARAMS ===============================

res_dir_proj <- "~/res_myocarditis"
res_dir_date <- "2021-10-08_fixglht"
dose_str <- "dose2"
mdl <- "mdl3b_fullyadj" # "mdl1_unadj", "mdl2_agesex", "mdl3a_bkwdselect", "mdl3b_fullyadj", "mdl4_fullinteract_suppl34", "mdl5_anydiag_death28days", "mdl4_fullinteract_suppl34"
# data_version <- "death28days" # death28days, anydiag
postexpo_groups <- c("") # , "ind_expo_"# for event_count
fmls <- c("+ weeks + agegroup + sex", "+ week*agegroup + sex", "+ week*sex + agegroup") # c("+ weeks + agegroup + sex", "+ week*agegroup + sex", "+ week*sex + agegroup")

agegp <- "all"
expo <- ifelse(dose_str == "dose2", "VAC2", "VAC1")

ls_events <- c("any_myo_or_pericarditis")
print(ls_events)

if (mdl == "mdl1_unadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "unadj_nosexforcombined")
} else if (mdl == "mdl2_agesex"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "adj_age_sex_only")
} else if (mdl == "mdl3a_bkwdselect"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "fully_adj_bkwdselect")
} else if (mdl == "mdl3b_fullyadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, dose_str, "fully_adj_bkwdselect")
  }
setwd(file.path(res_dir))


# ls_events <- ls_events[!ls_events %in% c("DIC", "TTP")]
#========================== HRs phenotypes ===============================
fmls <- c("+ weeks + agegroup + sex", "+ week*agegroup + sex", "+ week*sex + agegroup") # c("+ weeks + agegroup + sex", "+ week*agegroup + sex", "+ week*sex + agegroup")

combos <- expand.grid(ls_events, c("vac_az", "vac_pf"), "all", fmls)
names(combos) <- c("event", "vac_str", "agegp", "fml")

# paste0("tbl_hr_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv")
# paste0("tbl_event_count_ind_expo_" , expo, "_", event, "_", agegp, "_", vac_str,".csv")
# paste0("tbl_event_count_" , expo, "_", event, "_", agegp, "_", vac_str,".csv")

ls_hrs <- pmap(list(combos$event, combos$vac_str, combos$agegp, combos$fml), 
               function(event, vac_str, agegp, fml) 
                 file.path(res_dir, paste0("tbl_hr_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv")))



ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(combos)) {
  row <- combos[i,]
  fpath <- file.path(res_dir, paste0("tbl_hr_" , row$fml, "_", expo, "_", row$event, "_", row$agegp, "_", row$vac_str, ".csv"))
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
print(ls_events_missing)

if (dim(ls_events_missing)[1]>0){
  #  fread completed ones
  ls_hrs <- lapply(ls_events_done, fread)
  combos <- anti_join(combos, ls_events_missing)
}


ls_hrs <- lapply(ls_events_done, fread)



ls_hrs <- pmap(list(ls_events_done, combos$event, combos$vac_str, combos$agegp, combos$fml), 
               function(fpath, event, vac_str, agegp, fml){ 
                 df <- fread(fpath) 
                 df$event <- event
                 df$vac_str <- vac_str
                 df$agegp <- agegp
                 df$fml <- fml
                 return(df)
               })




df_hr <- rbindlist(ls_hrs, fill=TRUE) %>% dplyr::select_if(!names(.) == "V1")

df_hr <- df_hr %>% mutate_if(is.numeric, round, 4)


write.csv(df_hr, file = file.path(res_dir, "tbl_hr.csv") , row.names=F)

df_hr %>% View()



#========================== HRs glht ===============================
rm(df_hr, ls_hrs)
fmls <- c("+ week*agegroup + sex", "+ week*sex + agegroup") # c("+ weeks + agegroup + sex", "+ week*agegroup + sex", "+ week*sex + agegroup")

combos <- expand.grid(ls_events, c("vac_az", "vac_pf"), "all", fmls)
names(combos) <- c("event", "vac_str", "agegp", "fml")

# paste0("tbl_hr_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv")
# paste0("tbl_event_count_ind_expo_" , expo, "_", event, "_", agegp, "_", vac_str,".csv")
# paste0("tbl_event_count_" , expo, "_", event, "_", agegp, "_", vac_str,".csv")

ls_hrs <- pmap(list(combos$event, combos$vac_str, combos$agegp, combos$fml), 
               function(event, vac_str, agegp, fml) 
                 file.path(res_dir, paste0("tbl_hr_glht_" , fml, "_", expo, "_", event, "_", agegp, "_", vac_str, ".csv")))


ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(combos)) {
  row <- combos[i,]
  fpath <- file.path(res_dir, paste0("tbl_hr_glht_" , row$fml, "_", expo, "_", row$event, "_", row$agegp, "_", row$vac_str, ".csv"))
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
print(ls_events_missing)

if (dim(ls_events_missing)[1]>0){
  #  fread completed ones
  ls_hrs <- lapply(ls_events_done, fread)
  combos <- anti_join(combos, ls_events_missing)
}

ls_hrs <- lapply(ls_events_done, fread)



ls_hrs <- pmap(list(ls_events_done, combos$event, combos$vac_str, combos$agegp, combos$fml), 
               function(fpath, event, vac_str, agegp, fml){ 
                 df <- fread(fpath) 
                 df$event <- event
                 df$vac_str <- vac_str
                 df$agegp <- agegp
                 df$fml <- fml
                 return(df)
               })




df_hr <- rbindlist(ls_hrs, fill=TRUE) %>% dplyr::select_if(!names(.) == "V1")

df_hr <- df_hr %>% mutate_if(is.numeric, round, 4)


write.csv(df_hr, file = file.path(res_dir, "tbl_hr_glht.csv") , row.names=F)

df_hr %>% View()



# =============================  R events count =====================================
rm(ls_hrs)
ls_hrs <- pmap(list(combos$event, combos$agegp,  combos$vac_str),
               function(event, agegp, vac_str)
                 file.path(res_dir,
                           paste0("tbl_event_count_",
                                  expo,"_",
                                  event, "_",
                                  agegp, "_", vac_str, ".csv")
                 )
)
ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(combos)) {
  row <- combos[i,]
  fpath <- file.path(res_dir,
                     paste0("tbl_event_count_",
                            expo,"_",
                            row$event, "_",
                            row$agegp, "_", row$vac_str, ".csv"))
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
print(ls_events_missing)

#  fread completed ones
ls_hrs <- pmap(list(ls_events_done, combos$event, combos$agegp,  combos$vac_str), 
               function(fpath, event, agegp,vac_str){ 
                 df <- fread(fpath) 
                 df$event <- event
                 df$agegp <- agegp
                 df$vac <- vac_str
                 return(df)
               })



df_hr <- rbindlist(ls_hrs, fill=TRUE)  #%>% dplyr::select_if(!"V1")
df_hr <- df_hr %>% dplyr::select(event, agegp, vac,  expo_week, events_total, events_M, events_F)

write.csv(df_hr, file = file.path(res_dir, "event_count.csv") , row.names=F)





# # ============================= suppl tbl 3 & 4 ================================
# res_dir_date <- "2021-06-15"
# mdl <- "mdl4_fullinteract_suppl34" # mdl3b_fullyadj, mdl4_fullinteract_suppl34, mdl5_anydiag_death28days
# 
# setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/"))
# res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/")
# 
# ls_events <- c("Venous_event", "Arterial_event")
# ls_interacting_feats <- c("age_deci", "SEX", "CATEGORISED_ETHNICITY", "IMD", 
#                           "EVER_TCP", "EVER_THROMBOPHILIA", "EVER_PE_VT", "COVID_infection", 
#                           "COCP_MEDS", "HRT_MEDS", "ANTICOAG_MEDS", "ANTIPLATLET_MEDS", 
#                           "prior_ami_stroke", "EVER_DIAB_DIAG_OR_MEDS")
# 
# interactingfeat_age_vac_combos <- expand.grid(ls_interacting_feats, c("Venous_event", "Arterial_event"), c("vac_az", "vac_pf"))
# names(interactingfeat_age_vac_combos) <- c("interacting_feat", "event", "vac")
# ls_wald <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
#                        function(interacting_feat, event, vac_str) 
#                          paste0(res_dir,
#                                 "wald_",
#                                 interacting_feat, "_",
#                                 event, "_",
#                                 vac_str, ".csv"
#                          ))
# 
# ls_fit_tidy <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
#                function(interacting_feat, event, vac_str) 
#                  paste0(res_dir, "fit_ref_wald_tidy_",  interacting_feat,  "_", event,  "_", vac_str, ".csv"))
# 
# ls_fit_glht <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
#                     function(interacting_feat, event, vac_str) 
#                       paste0(res_dir, "fit_ref_wald_glht_",  interacting_feat,  "_", event,  "_", vac_str, ".csv"))
# 
# ls_wald <- unlist(ls_wald)
# ls_fit_tidy <- unlist(ls_fit_tidy)
# ls_fit_glht <- unlist(ls_fit_glht)
# 
# 
# ls_events_missing <- data.frame()
# ls_events_done <- c()
# for (i in 1:nrow(interactingfeat_age_vac_combos)) {
#   row <- interactingfeat_age_vac_combos[i,]
#   fpath <- paste0(res_dir,
#                   "wald_",
#                   row$interacting_feat, "_",
#                   row$event, "_",
#                   row$vac_str, ".csv"
#   )
#   
#   if (!file.exists(fpath)) {
#     ls_events_missing <- rbind(ls_events_missing, row)
#   } else {
#     ls_events_done <- c(ls_events_done, fpath)
#   }
# }
# 
# # which ones are missing?
# ls_events_missing %>% View()
# print(ls_events_missing)
# 
# #  fread completed ones
# ls_wald <- lapply(ls_wald, fread)
# ls_fit_tidy <- lapply(ls_fit_tidy, fread)
# ls_fit_glht <- lapply(ls_fit_glht, fread)
# 
# # ...... wald ......
# df_wald <- rbindlist(ls_wald, fill=TRUE)
# df_wald <- df_wald %>% filter(! is.na(statistic))
# df_wald <- df_wald %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "interacting_feat", "vac_str", "week", 
#                                                                                 "p.value", "df", "res.df")
# write.csv(df_wald, file = paste0(res_dir, "hrs_vac_wald.csv") , row.names=F)
# 
# # ...... fit_tidy ......
# ls_fit_tidy <- map2(split(interactingfeat_age_vac_combos,seq(nrow(interactingfeat_age_vac_combos))), ls_fit_tidy, 
#                     function(feats, df) 
#                       {
#                       df$event <- feats$event
#                       df$interacting_feat <- feats$interacting_feat
#                       df$vac <- feats$vac
#                       return(df)
#                       }
#                     )
# 
# df_fit_tidy <- rbindlist(ls_fit_tidy, fill=TRUE)
# df_fit_tidy <- df_fit_tidy %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "interacting_feat", "vac", 
#                                                                             "term", "estimate", "conf.low", "conf.high", "p.value", "std.error", "robust.se", 
#                                                                             "statistic"
#                                                                             )
# write.csv(df_fit_tidy, file = paste0(res_dir, "hrs_vac_fit_tidy.csv") , row.names=F)
# 
# # ...... fit_glht ......
# df_fit_glht <- rbindlist(ls_fit_glht, fill=TRUE)
# df_fit_glht <- df_fit_glht %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "interacting_feat", "vac_str",
#                                                                                         "contrast", "estimate", "std.error", "adj.p.value", 
#                                                                                         "statistic", "null.value"
#                                                                                         )
# write.csv(df_fit_glht, file = paste0(res_dir, "hrs_vac_fit_glht.csv") , row.names=F)
