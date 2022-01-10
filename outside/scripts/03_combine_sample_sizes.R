rm(list = ls())

library(magrittr)

# Load English data ------------------------------------------------------------

england1 <- read_excel("raw/england/D1D2_counts_20220107.xlsx", 
                       sheet = "D1", 
                       col_names = FALSE, 
                       col_types = c("skip", "text", "numeric"))

colnames(england1) <- c("D1","England")

england2 <- read_excel("raw/england/D1D2_counts_20220107.xlsx",
                      sheet = "D2", 
                      col_types = c("skip", "text", "numeric", "numeric", "numeric", "numeric"), 
                      skip = 1)

colnames(england2) <- c("D1","AstraZeneca","Moderna","none","Pfizer")

england2 <- tidyr::pivot_longer(england2, cols = c("AstraZeneca","Moderna","none","Pfizer"))

colnames(england2) <- c("D1","D2","England")

# Load Welsh data --------------------------------------------------------------

wales2 <- readxl::read_excel("raw/wales/D1 D2 counts.xlsx",
                            sheet = "D2", 
                            col_types = c("skip", "text","numeric", "numeric", "numeric","numeric"),
                            skip = 1)

colnames(wales2) <- c("D1","AstraZeneca","Moderna","Pfizer","none")

wales2 <- tidyr::pivot_longer(wales2, cols = c("AstraZeneca","Moderna","Pfizer","none"))

colnames(wales2) <- c("D1","D2","Wales")
wales2$Wales <- ifelse(is.na(wales2$Wales),0,wales2$Wales)

wales1 <- wales2[,c("D1","Wales")]
wales1 <- aggregate(Wales ~ D1, data = wales1, sum, na.rm = TRUE)

# Summarise dose 1 -------------------------------------------------------------

df1 <- merge(england1, wales1, by = "D1")
df1$total <- df1$England + df1$Wales
colnames(df1) <- c("exposure","England","Wales","total")
df1$exposure <- ifelse(df1$exposure %in% c("AstraZeneca","Pfizer"),df1$exposure ,"Comparator")
df1 <- aggregate(. ~ exposure, data = df1, sum, na.rm = TRUE)
df1 <- rbind(df1,c("Total",sum(df1$England),sum(df1$Wales),sum(df1$total)))
df1$analysis <- "Dose 1"

# Summarise dose 2 -------------------------------------------------------------

df2 <- merge(england2, wales2, by = c("D1","D2"))
df2$total <- df2$England + df2$Wales
df2$D1 <- gsub("none","None",df2$D1)
df2$D2 <- gsub("none","None",df2$D2)
df2 <- df2[df2$D1!="None",]
df2$exposure <- ifelse(df2$D1 %in% c("AstraZeneca","Pfizer") & df2$D1==df2$D2,df2$D1 ,"Comparator")
df2 <- df2[,c("exposure","England","Wales","total")]

df2 <- aggregate(. ~ exposure, data = df2, sum, na.rm = TRUE)

df2 <- rbind(df2,c("Total",sum(df2$England),sum(df2$Wales),sum(df2$total)))

df2$analysis <- "Dose 2"

# Format table -----------------------------------------------------------------

doses <- rbind(df1,df2)

doses$exposure <- dplyr::recode(doses$exposure, "Pfizer" = "BNT162b2", "AstraZeneca" = "ChAdOx1-S", "Comparator" = "Comparator", "Total" = "Total")
doses$exposure <- factor(doses$exposure, levels=c("BNT162b2", "ChAdOx1-S", "Comparator", "Total"))

doses <- doses[order(doses$analysis,doses$exposure),c("analysis","exposure","England","Wales","total")]

# Save -------------------------------------------------------------------------

data.table::fwrite(doses,"output/doses.csv")