rm(list = ls())

library(magrittr)

# Load data --------------------------------------------------------------------

df <- data.table::fread("raw/england/doses.csv", data.table = FALSE)

colnames(df) <- c("D1","D2","N")

df$D1 <- ifelse(df$D1=="null","None",df$D1)
df$D2 <- ifelse(df$D2=="null","None",df$D2)

# Dose 1 -----------------------------------------------------------------------

df1 <- df[,c("D1","N")]
df1$D1 <- ifelse(df1$D1 %in% c("AstraZeneca","Pfizer"),df1$D1,"Neither")
df1 <- aggregate(. ~ D1, data = df1, sum, na.rm = TRUE)
colnames(df1) <- c("analysis_product","N")
df1$sample <- "Exposed"
tmp <- df1[,c("sample","N")]
tmp <- aggregate(. ~ sample, data = tmp, sum, na.rm = TRUE)
tmp$sample <- "Total"
tmp$analysis_product <-"AstraZeneca"
df1 <- rbind(df1,tmp)
tmp$analysis_product <-"Pfizer"
df1 <- rbind(df1,tmp)
df1 <- df1[df1$analysis_product!="Neither",]
df1$analysis_dose <- "Dose 1"

# Summarise dose 2 -------------------------------------------------------------

df2 <- df
df2 <- df2[!(df2$D1 %in% c("None","Moderna")),]
tmp <- df2[df2$D1==df2$D2,]
tmp$sample <- "Exposed"
df2$sample <- "Total"
df2 <- rbind(df2,tmp)
df2$D2 <- NULL
df2 <- aggregate(. ~ D1 + sample, data = df2, sum, na.rm = TRUE)
df2 <- dplyr::rename(df2, "analysis_product" = "D1")
df2$analysis_dose <- "Dose 2"

# Format table -----------------------------------------------------------------

doses <- rbind(df1,df2)

doses$analysis_product <- dplyr::recode(doses$analysis_product, "Pfizer" = "BNT162b2", "AstraZeneca" = "ChAdOx1-S")
doses$analysis_product <- factor(doses$analysis_product, levels=c("BNT162b2", "ChAdOx1-S"))

doses <- doses[order(doses$analysis_dose,doses$analysis_product),c("analysis_dose","analysis_product","sample","N")]

# Save -------------------------------------------------------------------------

data.table::fwrite(doses,"output/doses.csv")