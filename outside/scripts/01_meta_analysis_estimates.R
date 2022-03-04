rm(list = ls())

# Identify files containing estimates ------------------------------------------

files <- c(list.files(path = "raw/england", pattern = "tbl_hr_", full.names = TRUE))

# Combine files in a single data frame -----------------------------------------

df <- NULL

for (f in files) {
  
  # Load data ------------------------------------------------------------------
  
  tmp <- data.table::fread(f, data.table = FALSE)
  
  # Add meta data --------------------------------------------------------------

  tmp$outcome <- ifelse(grepl("_pericarditis_",f),
                        "pericarditis",
                        ifelse(grepl("_myocarditis_",f),
                               "myocarditis",
                               ifelse(grepl("_myopericarditis_",f),
                                      "myocarditis/pericarditis","")))
  tmp$nation <- stringr::str_to_title(gsub("/.*","",gsub("raw/","",f)))
  tmp$source <- f
  tmp$dose <- paste0("Dose ",substr(gsub(".*VAC","",f),1,1))
  tmp$vac_str <- gsub(".*all_","",gsub(".csv","",f))
  
  tmp$priorcovid <- "All"
  tmp$priorcovid <- ifelse(grepl("priorcovid0",f),"No",tmp$priorcovid)
  tmp$priorcovid <- ifelse(grepl("priorcovid1",f),"Yes",tmp$priorcovid)

  tmp$interaction <- FALSE
    
  # For English interaction data, add CI and term ------------------------------
  
  if (grepl("glht",f)) {
    tmp$term <- tmp$contrast
    tmp$fml <- gsub(".*week","week",gsub("_VAC.*","",f))
    tmp$conf.low <- exp(tmp$estimate-qnorm(0.975)*tmp$std.error)
    tmp$conf.high <- exp(tmp$estimate+qnorm(0.975)*tmp$std.error)
    tmp$estimate <- exp(tmp$estimate)
    tmp$robust.se <- tmp$std.error
    tmp$p.value <- tmp$adj.p.value
    tmp$interaction <- TRUE
  }
  
  # Tidy data ------------------------------------------------------------------
  
  tmp <- tmp[,c("outcome","nation","dose","vac_str","term","fml","estimate","conf.low","conf.high","robust.se","p.value","priorcovid","source","interaction")]
  tmp <- tmp[grepl("week",tmp$term),]
  
  # Append to master data frame ------------------------------------------------
  
  df <- plyr::rbind.fill(df,tmp)
  
}

# Remove components to calculate interactions ----------------------------------

df <- df[df$interaction==TRUE | (df$interaction==FALSE & df$fml=="+ weeks + agegroup + sex"),]
df$interaction <- NULL

# Remove empty estimates -------------------------------------------------------

df <- df[!is.na(df$estimate),]

# Label days post vaccination --------------------------------------------------

df$days_post_vaccination <- gsub(" ","",substr(df$term,1,8))

df$days_post_vaccination  <- factor(df$days_post_vaccination,
                                    levels=c("week1_2", "week3_23", "week1_23"))

df$days_post_vaccination  <- dplyr::recode(df$days_post_vaccination, 
                                           "week1_2" = "0-13", 
                                           "week3_23"="14+", 
                                           "week1_23" = "0+")

# Label age group --------------------------------------------------------------

df$age_group <- "All"
df$age_group <- ifelse(df$fml=="week*agegroup + sex","40-69",df$age_group)
df$age_group <- ifelse(df$fml=="week*agegroup + sex" & grepl("agegroup0to40",df$term),"<40",df$age_group)
df$age_group <- ifelse(df$fml=="week*agegroup + sex" & grepl("agegroup70to500",df$term),"70+",df$age_group)

# Label sex --------------------------------------------------------------------

df$sex <- "All"
df$sex <- ifelse(df$fml=="week*sex + agegroup","Male",df$sex)
df$sex <- ifelse(df$fml=="week*sex + agegroup" & grepl("SEX2",df$term),"Female",df$sex)

# Label vaccine type -----------------------------------------------------------

df$vaccination_product <- factor(df$vac_str, levels=c("vac_az", "vac_pf"))
df$vaccination_product <- dplyr::recode(df$vaccination_product , "vac_az" = "ChAdOx1-S", "vac_pf"="BNT162b2")

# Save -------------------------------------------------------------------------

df <- df[,c("outcome","nation","dose","age_group","sex","vaccination_product","days_post_vaccination","priorcovid","estimate","conf.low","conf.high","p.value")]
colnames(df) <- c("outcome","nation","dose","age_group","sex","exposure","days_post_vaccination","prior_covid","estimate","conf.low","conf.high","p.value")
data.table::fwrite(df,"output/estimates.csv")