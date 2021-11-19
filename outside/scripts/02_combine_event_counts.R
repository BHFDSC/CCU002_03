rm(list = ls())

library(magrittr)

# Load English dose 1 counts ---------------------------------------------------

counts1 <- data.table::fread("raw/England_dose1_events.csv",
                             select = c("vac","expo_week","events_total"),
                             data.table = FALSE)

counts1$vac <- ifelse(grepl("week", counts1$expo_week),counts1$vac,"none_or_before")
counts1 <- unique(counts1)
counts1 <- aggregate(events_total ~ vac + expo_week, data = counts1, sum, na.rm = TRUE) 
counts1$dose <- "Dose 1"
counts1 <- counts1[counts1$expo_week!="all post expo",]

# Load English dose 2 counts ---------------------------------------------------

counts2 <- data.table::fread("raw/England_dose2_events.csv",
                             select = c("vac","expo_week","events_total"),
                             data.table = FALSE)

counts2$vac <- ifelse(grepl("week", counts2$expo_week),counts2$vac,"none_or_before")
counts2 <- unique(counts2)
counts2 <- aggregate(events_total ~ vac + expo_week, data = counts2, sum, na.rm = TRUE)
counts2$dose <- "Dose 2"
counts2 <- counts2[counts2$expo_week!="all post expo",]

# Combine English counts -------------------------------------------------------

counts <- rbind(counts1, counts2)
counts$nation <- "England"

# Label days post vaccination ------------------------------------------------------------------

counts$days_post_vaccination <- ifelse(counts$expo_week=="pre expo","Before",counts$expo_week)
counts$days_post_vaccination  <- factor(counts$days_post_vaccination ,levels=c("Before","week1_2", "week3_23"))
counts$days_post_vaccination  <- dplyr::recode(counts$days_post_vaccination , "week1_2" = "0-13", "week3_23"="14+")

# Label vaccine type -----------------------------------------------------------

counts$vaccination_product <- factor(counts$vac, levels=c("none_or_before","vac_az", "vac_pf"))
counts$vaccination_product <- dplyr::recode(counts$vaccination_product, "none_or_before" = "Not applicable", "vac_az" = "ChAdOx1-S", "vac_pf"="BNT162b2")

# Save counts ------------------------------------------------------------------

counts <- counts[,c("nation","dose","vaccination_product","days_post_vaccination","events_total")]
colnames(counts) <- c("nation","dose","vaccination_product","days_post_vaccination","events")
data.table::fwrite(counts,"output/counts.csv")