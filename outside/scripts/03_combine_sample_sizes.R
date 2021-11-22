rm(list = ls())

library(magrittr)

# Load English data ------------------------------------------------------------

england <- readxl::read_excel("raw/England_D1D2.xlsx",
                              sheet = "D2", 
                              col_types = c("skip", "text","numeric", "numeric", "numeric","numeric"),
                              skip = 1)

colnames(england) <- c("D1","AstraZeneca","Moderna","none","Pfizer")

england <- tidyr::pivot_longer(england, cols = c("AstraZeneca","Moderna","none","Pfizer"))

colnames(england) <- c("D1","D2","England")

# Load Welsh data --------------------------------------------------------------

wales <- readxl::read_excel("raw/Wales_D1D2.xlsx",
                            sheet = "D2", 
                            col_types = c("skip", "text","numeric", "numeric", "numeric","numeric"),
                            skip = 1)

colnames(wales) <- c("D1","AstraZeneca","Moderna","Pfizer","none")

wales <- tidyr::pivot_longer(wales, cols = c("AstraZeneca","Moderna","Pfizer","none"))

colnames(wales) <- c("D1","D2","Wales")
wales$Wales <- ifelse(is.na(wales$Wales),0,wales$Wales)
#wales$Wales <- ifelse(wales$D1=="AstraZeneca" & wales$D2=="Pfizer",0,wales$Wales)

# Combine results from both nations --------------------------------------------

df <- merge(england, wales, by = c("D1","D2"))
df$total <- df$England + df$Wales
df$D1 <- gsub("none","None",df$D1)
df$D2 <- gsub("none","None",df$D2)
df <- df[df$D1!="None" | (df$D1=="None" & df$D2=="None"),]

# Summarise dose 1 -------------------------------------------------------------

df1 <- df[,c("D1","England","Wales","total")]
colnames(df1) <- c("exposure","England","Wales","total")

df1$exposure <- ifelse(df1$exposure  %in% c("AstraZeneca","Pfizer"),df1$exposure ,"Comparator")

df1 <- df1 %>%
  dplyr::group_by(exposure) %>%
  dplyr::summarise(England = sum(England),
                   Wales = sum(Wales),
                   total = sum(total)) %>%
  dplyr::ungroup()

df1 <- rbind(df1,c("Total",sum(df1$England),sum(df1$Wales),sum(df1$total)))

df1$analysis <- "Dose 1"

# Summarise dose 2 -------------------------------------------------------------

df2 <- df[df$D1!="None" & df$D2 %in% c("AstraZeneca","Pfizer","None"),c("D2","England","Wales","total")]
colnames(df2) <- c("exposure","England","Wales","total")

df2$exposure <- ifelse(df2$exposure  %in% c("AstraZeneca","Pfizer"),df2$exposure ,"Comparator")

df2 <- df2 %>%
  dplyr::group_by(exposure) %>%
  dplyr::summarise(England = sum(England),
                   Wales = sum(Wales),
                   total = sum(total)) %>%
  dplyr::ungroup()

df2 <- rbind(df2,c("Total",sum(df2$England),sum(df2$Wales),sum(df2$total)))

df2$analysis <- "Dose 2"

# Format table -----------------------------------------------------------------

doses <- rbind(df1,df2)

doses$exposure <- dplyr::recode(doses$exposure, "Pfizer" = "BNT162b2", "AstraZeneca" = "ChAdOx1-S", "Comparator" = "Comparator", "Total" = "Total")
doses$exposure <- factor(doses$exposure, levels=c("BNT162b2", "ChAdOx1-S", "Comparator", "Total"))

doses <- doses[order(doses$analysis,doses$exposure),c("analysis","exposure","England","Wales","total")]

# Save -------------------------------------------------------------------------

data.table::fwrite(doses,"output/doses.csv")