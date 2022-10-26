rm(list = ls())

source("scripts/specify_paths.R")

# Identify files containing estimates ------------------------------------------

files <- list.files(path = results, 
                    pattern = "tbl_hr_", 
                    full.names = TRUE, 
                    recursive = TRUE)

files <- gsub(paste0(results,"/"),"",files)
files <- files[!grepl("Legacy/",files)]
files <- files[!grepl("glht",files)]

# Combine files in a single data frame -----------------------------------------

df <- NULL

for (f in files) {
  
  # Load data ------------------------------------------------------------------
  
  tmp <- data.table::fread(paste0(results,"/",f), data.table = FALSE)
  
  if (nrow(tmp)>0) {
  
    # Add meta data ------------------------------------------------------------
    
    tmp$source <- f
    tmp$nation <- "England"
    
    tmp$priorcovid <- "All"
    tmp$priorcovid <- ifelse(grepl("priorcovid0",f),"No",tmp$priorcovid)
    tmp$priorcovid <- ifelse(grepl("priorcovid1",f),"Yes",tmp$priorcovid)
    
    tmp$extended_fup <- grepl("extended_end_date",f)
  
    tmp <- dplyr::rename(tmp, "outcome" = "event")
    tmp <- tmp[grepl("day",tmp$term),]
    
    # Append to master data frame ----------------------------------------------
    
    df <- plyr::rbind.fill(df,tmp)
    
  }
  
}

df <- tidyr::separate(data = df, 
                      col = source, 
                      sep = "/", 
                      into = c("analysis","dose","mdl","tbl"),
                      remove = FALSE)

# Remove interaction components and label interaction terms --------------------

df <- df[!grepl(":",df$term),]

df$interaction <- gsub(" \\+.*","",substr(df$fml,3,length(df$fml)))
df$interaction <- ifelse(df$interaction=="weeks","none",df$interaction)
df$interaction <- ifelse(df$interaction=="week*agegroup","agegroup",df$interaction)
df$interaction <- ifelse(df$interaction=="week*sex","sex",df$interaction)

# Remove empty estimates -------------------------------------------------------

df <- df[!is.na(df$estimate),]

# Label age group --------------------------------------------------------------

df <- dplyr::rename(df, "age_group" = "agegp")
df$age_group <- ifelse(df$age_group=="all","All",df$age_group)
df$age_group <- ifelse(is.na(df$age_group),"All",df$age_group)
df$age_group <- ifelse(df$age_group==">=70","70+",df$age_group)
df$age_group <- ifelse(df$fml=="weekagegroup + sex","40-69",df$age_group)
df$age_group <- ifelse(df$fml=="weekagegroup + sex" & grepl("agegroup0to40",df$term),"<40",df$age_group)
df$age_group <- ifelse(df$fml=="weekagegroup + sex" & grepl("agegroup70to500",df$term),"70+",df$age_group)
df$age_group <- ifelse(df$fml=="weeksex + agegroup","All",df$age_group)

# Label sex --------------------------------------------------------------------

df$sex <- ifelse(df$sex=="all", "All", df$sex)
df$sex <- ifelse(is.na(df$sex), "All", df$sex)
df$sex <- ifelse(df$sex=="1", "Male", df$sex)
df$sex <- ifelse(df$sex=="2", "Female", df$sex)
df$sex <- ifelse(df$fml=="weeksex + agegroup","Male",df$sex)
df$sex <- ifelse(df$fml=="weeksex + agegroup" & grepl("SEX2",df$term),"Female",df$sex)

# Label vaccine type -----------------------------------------------------------

df$vaccination_product <- as.character(grepl("vac_az",df$tbl))
df$vaccination_product <- dplyr::recode(df$vaccination_product , "TRUE" = "ChAdOx1-S", "FALSE" = "BNT162b2")

# Label doses ------------------------------------------------------------------

df$dose <- gsub("dose","Dose ", df$dose)

# Label outcomes ---------------------------------------------------------------

df$outcome <- gsub("any_","",df$outcome)
df$outcome <- paste0(toupper(substr(df$outcome,1,1)),substr(df$outcome,2,nchar(df$outcome)))

# Save -------------------------------------------------------------------------

df <- df[,c("source","fml","nation","outcome","dose","age_group","sex","extended_fup","vaccination_product","priorcovid","interaction","term","estimate","conf.low","conf.high","p.value")]
colnames(df) <- c("source","fml","nation","outcome","dose","age_group","sex","extended_fup","exposure","prior_covid","interaction","term","estimate","conf.low","conf.high","p.value")
data.table::fwrite(df,"output/estimates.csv")