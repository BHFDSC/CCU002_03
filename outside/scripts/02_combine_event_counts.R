rm(list = ls())

source("scripts/specify_paths.R")

library(magrittr)

# List English count files -----------------------------------------------------

files <- list.files(path = results, 
                    pattern = "tbl_event_count_", 
                    full.names = TRUE, 
                    recursive = TRUE)

files <- gsub(paste0(results,"/"),"",files)

# Combine files in a single data frame -----------------------------------------

counts <- NULL

for (f in files) {
  
  # Load data ------------------------------------------------------------------
  
  tmp <- data.table::fread(paste0(results,"/",f),
                           #select = c("expo_day","events_total"),
                           data.table = FALSE)
  
  # Add meta data --------------------------------------------------------------
  
  tmp$source <- f
  tmp$nation <- "England"
  
  tmp$priorcovid <- "All"
  tmp$priorcovid <- ifelse(grepl("priorcovid0",f),"No",tmp$priorcovid)
  tmp$priorcovid <- ifelse(grepl("priorcovid1",f),"Yes",tmp$priorcovid)
  
  # Append to master data frame ------------------------------------------------
  
  counts <- plyr::rbind.fill(counts,tmp)
  
}

counts <- tidyr::separate(data = counts, 
                          col = source, 
                          sep = "/", 
                          into = c("analysis","dose","mdl","tbl"),
                          remove = FALSE)

# Label age group --------------------------------------------------------------

counts$age_group <- "All"
counts$age_group <- ifelse(grepl("40-69",counts$tbl),"40-69",counts$age_group)
counts$age_group <- ifelse(grepl("over70",counts$tbl),"70+",counts$age_group)
counts$age_group <- ifelse(grepl("under40",counts$tbl),"<40",counts$age_group)

# Label vaccine type -----------------------------------------------------------

counts$vaccination_product <- as.character(grepl("vac_az",counts$tbl))
counts$vaccination_product <- dplyr::recode(counts$vaccination_product , "TRUE" = "ChAdOx1-S", "FALSE" = "BNT162b2")

# Label extended follow-up -----------------------------------------------------

counts$extended_fup <- grepl("extended",counts$analysis)

# Label doses ------------------------------------------------------------------

counts$dose <- gsub("dose","Dose ", counts$dose)

# Label outcomes ---------------------------------------------------------------

counts$outcome <- ""
counts$outcome <- ifelse(grepl("myocarditis",counts$tbl), "Myocarditis", counts$outcome)
counts$outcome <- ifelse(grepl("pericarditis",counts$tbl), "Pericarditis", counts$outcome)

# Label term -------------------------------------------------------------------

counts$expo_day <- ifelse(counts$expo_day=="pre_expo","before",counts$expo_day)

# Save counts ------------------------------------------------------------------

counts <- counts[,c("nation","outcome","dose","age_group","extended_fup","vaccination_product","expo_day","priorcovid","events_total")]
colnames(counts) <- c("nation","outcome","dose","age_group","extended_fup","exposure","term","prior_covid","events")
counts <- unique(counts)

data.table::fwrite(counts,"output/hidden_counts.csv")

tmp <- counts[counts$term!="before" &
                counts$prior_covid=="All" & 
                counts$age_group=="All" & 
                counts$nation=="England",
              c("outcome","dose","extended_fup","exposure","term","prior_covid","events")]

data.table::fwrite(tmp,"output/counts.csv")

# Table 2 ----------------------------------------------------------------------

table2 <- counts[counts$prior_covid=="All" & 
                   counts$age_group=="All" & 
                   counts$nation=="England" &
                   counts$outcome=="Myocarditis",
                 c("dose","extended_fup","exposure","term","events")]

table2$term <- ifelse(table2$term %in% c("day14_412","day14_161"),
                      "day14+",table2$term)

table2 <- tidyr::pivot_wider(table2,
                             id_cols = c("exposure","extended_fup"),
                             names_from = c("dose","term"),
                             values_from = "events")

table2 <- table2[order(table2$exposure,table2$extended_fup),]

data.table::fwrite(table2, "output/table2.csv", row.names = FALSE)