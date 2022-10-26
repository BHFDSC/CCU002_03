rm(list = ls())

library(magrittr)

# List English count files -----------------------------------------------------

files <- c(list.files(path = "raw/england", pattern = "wald", full.names = TRUE))

# Combine files in a single data frame -----------------------------------------

df <- NULL

for (f in files) {
  
  # Load data ------------------------------------------------------------------
  
  tmp <- data.table::fread(f, data.table = FALSE)
  tmp <- tmp[!is.na(tmp$p.value),]
  
  # Add meta data --------------------------------------------------------------
  
  tmp$outcome <- ifelse(grepl("_pericarditis_",f),
                        "pericarditis",
                        ifelse(grepl("_myocarditis_",f),
                               "myocarditis",
                               ifelse(grepl("_myopericarditis_",f),
                                      "myocarditis/pericarditis","")))
  
  tmp$dose <- paste0("Dose ",substr(gsub(".*VAC","",f),1,1))
  tmp$exposure <- gsub(".*all_","",gsub(".csv","",f))
  
  tmp$priorcovid <- "All"
  tmp$priorcovid <- ifelse(grepl("priorcovid0",f),"No",tmp$priorcovid)
  tmp$priorcovid <- ifelse(grepl("priorcovid1",f),"Yes",tmp$priorcovid)
  
  # Append to master data frame ------------------------------------------------
  
  df <- plyr::rbind.fill(df,tmp)
  
}

# Label days post vaccination ------------------------------------------------------------------

df$days_post_vaccination <- df$week
df$days_post_vaccination <- ifelse(grepl("week1_2",df$days_post_vaccination ),"week1_2",df$days_post_vaccination )
df$days_post_vaccination  <- ifelse(grepl("week3_23",df$days_post_vaccination ),"week3_23",df$days_post_vaccination )
df$days_post_vaccination  <- factor(df$days_post_vaccination ,levels=c("week1_2", "week3_23"))
df$days_post_vaccination  <- dplyr::recode(df$days_post_vaccination , "week1_2" = "0-13", "week3_23"="14+")

# Label vaccine type -----------------------------------------------------------

df$exposure <- factor(df$vac_str, levels=c("vac_az", "vac_pf"))
df$exposure <- dplyr::recode(df$exposure , "vac_az" = "ChAdOx1-S", "vac_pf"="BNT162b2")

# Label interacting feature ----------------------------------------------------

df$interacting_feature <- factor(df$interacting_feat, levels=c("agegroup", "SEX"))
df$interacting_feature <- dplyr::recode(df$interacting_feature, "agegroup" = "Age group", "SEX" = "Sex")

# Save  ------------------------------------------------------------------------

df <- df[,c("outcome","dose","priorcovid","exposure","days_post_vaccination","interacting_feature","p.value")]
data.table::fwrite(df,"output/waldtests.csv")