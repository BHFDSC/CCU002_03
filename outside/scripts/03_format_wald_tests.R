rm(list = ls())

# Load data --------------------------------------------------------------------

dose1 <- data.table::fread("raw/wald_VAC1_myoperi.csv", data.table = FALSE)
dose1$dose <- "Dose 1"

dose2 <- data.table::fread("raw/wald_VAC2_myoperi.csv", data.table = FALSE)
dose2$dose <- "Dose 2"

df <- rbind(dose1,dose2)

# Label days post vaccination ------------------------------------------------------------------

df$days_post_vaccination <- df$week
df$days_post_vaccination <- ifelse(grepl("week1_2",df$days_post_vaccination ),"week1_2",df$days_post_vaccination )
df$days_post_vaccination  <- ifelse(grepl("week3_23",df$days_post_vaccination ),"week3_23",df$days_post_vaccination )
df$days_post_vaccination  <- factor(df$days_post_vaccination ,levels=c("week1_2", "week3_23"))
df$days_post_vaccination  <- dplyr::recode(df$days_post_vaccination , "week1_2" = "0-13", "week3_23"="14+")

# Label vaccine type -----------------------------------------------------------

df$vaccination_product <- factor(df$vac_str, levels=c("vac_az", "vac_pf"))
df$vaccination_product <- dplyr::recode(df$vaccination_product , "vac_az" = "ChAdOx1-S", "vac_pf"="BNT162b2")

# Label interacting feature ----------------------------------------------------

df$interacting_feature <- factor(df$interacting_feat, levels=c("agegroup", "SEX"))
df$interacting_feature <- dplyr::recode(df$interacting_feature, "agegroup" = "Age group", "SEX" = "Sex")

# Save  ------------------------------------------------------------------------

df <- df[,c("dose","vaccination_product","days_post_vaccination","interacting_feature","p.value")]
data.table::fwrite(df,"output/waldtests.csv")