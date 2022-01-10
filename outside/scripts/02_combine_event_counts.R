rm(list = ls())

library(magrittr)

# List English count files -----------------------------------------------------

files <- c(list.files(path = "raw/england", pattern = "tbl_event_count_", full.names = TRUE))

# Combine files in a single data frame -----------------------------------------

counts <- NULL

for (f in files) {
  
  # Load data ------------------------------------------------------------------
  
  tmp <- data.table::fread(f, 
                           select = c("expo_week","events_total"),
                           data.table = FALSE)
  
  # Add meta data --------------------------------------------------------------
  
  tmp$nation <- stringr::str_to_title(gsub("/.*","",gsub("raw/","",f)))
  tmp$dose <- paste0("Dose ",gsub(".*?([0-9]+).*", "\\1", f))
  tmp$vac_str <- gsub(".*all_","",gsub(".csv","",f))
  
  tmp$priorcovid <- "Mixed"
  tmp$priorcovid <- ifelse(grepl("priorcovid0",f),"No",tmp$priorcovid)
  tmp$priorcovid <- ifelse(grepl("priorcovid1",f),"Yes",tmp$priorcovid)

  # Append to master data frame ------------------------------------------------
  
  counts <- plyr::rbind.fill(counts,tmp)
  
}

counts <- counts[counts$expo_week!="all post expo",]
counts$vac_str <- ifelse(counts$expo_week=="pre expo","none_or_before",counts$vac_str)
counts <- unique(counts)

# Load Welsh counts ------------------------------------------------------------

counts_wales <- readxl::read_excel("raw/wales/event_counts_by_vacc-estimatedMasked_vmw.xlsx",
                              sheet = "event_counts_by_vacc",
                              col_types = c("text","text", "text", "skip", "numeric",
                                            "skip", "skip", "skip"), skip = 10)

counts_wales <- counts_wales[!(counts_wales$Vaccine_dose=="dose 2" & counts_wales$Vaccine_type=="UV" & counts_wales$Exposure_week=="pre expo"),]
counts_wales$Vaccine_type <- ifelse(counts_wales$Exposure_week=="pre expo","none_or_before",counts_wales$Vaccine_type)
counts_wales$Vaccine_type <- ifelse(counts_wales$Vaccine_type=="AZ","vac_az",counts_wales$Vaccine_type)
counts_wales$Vaccine_type <- ifelse(counts_wales$Vaccine_type=="Pf","vac_pf",counts_wales$Vaccine_type)
counts_wales$Vaccine_dose <- gsub("dose","Dose",counts_wales$Vaccine_dose)
counts_wales$Exposure_week <- gsub("-","_",counts_wales$Exposure_week)
colnames(counts_wales) <- c("dose","vac_str","expo_week","events_total")
counts_wales <- aggregate(events_total ~ vac_str + expo_week + dose, data = counts_wales, sum, na.rm = TRUE)
counts_wales$nation <- "Wales"
counts_wales$priorcovid <- "Mixed"
counts <- rbind(counts, counts_wales)

# Combine Welsh and English counts ---------------------------------------------

meta <- aggregate(events_total ~ vac_str + expo_week + dose + priorcovid, data = counts, sum, na.rm = TRUE)
meta$nation <- "All"
counts <- rbind(counts, meta)

# Label days post vaccination --------------------------------------------------

counts$days_post_vaccination <- ifelse(counts$expo_week=="pre expo","Before",counts$expo_week)
counts$days_post_vaccination  <- factor(counts$days_post_vaccination ,levels=c("Before","week1_2", "week3_23"))
counts$days_post_vaccination  <- dplyr::recode(counts$days_post_vaccination , "week1_2" = "0-13", "week3_23"="14+")

# Label vaccine type -----------------------------------------------------------

counts$vaccination_product <- factor(counts$vac_str, levels=c("none_or_before","vac_az", "vac_pf"))
counts$vaccination_product <- dplyr::recode(counts$vaccination_product, "none_or_before" = "Not applicable", "vac_az" = "ChAdOx1-S", "vac_pf"="BNT162b2")

# Save counts ------------------------------------------------------------------

counts <- counts[c("nation","dose","vaccination_product","days_post_vaccination","priorcovid","events_total")]
colnames(counts) <- c("nation","dose","exposure","days_post_vaccination","prior_covid","events")
data.table::fwrite(counts,"output/hidden_counts.csv")
data.table::fwrite(counts[counts$prior_covid=="Mixed" & counts$nation=="All",c("dose","exposure","days_post_vaccination","prior_covid","events")],"output/counts.csv")