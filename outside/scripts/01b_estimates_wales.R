# Load data --------------------------------------------------------------------

df <- data.table::fread("raw/wales/ccu002_03_wales.csv", data.table = FALSE)

# Tidy data --------------------------------------------------------------------

df$outcome <- "myocarditis/pericarditis"
df$nation <- "Wales"
df$dose <- gsub("Dose","Dose ",gsub(" .*","",df$vacdose))
df$age_group <- "All"
df$sex <- "All"
df$exposure <- ""
df$exposure <- ifelse(grepl("ChAdOx1",df$vacdose),"ChAdOx1-S",df$exposure)
df$exposure <- ifelse(grepl("BNT162b2",df$vacdose),"BNT162b2",df$exposure)
df$days_post_vaccination <- ""
df$days_post_vaccination <- ifelse(df$expo_week=="pre expo","Before",df$days_post_vaccination)
df$days_post_vaccination <- ifelse(df$expo_week=="week1_2","0-13",df$days_post_vaccination)
df$days_post_vaccination <- ifelse(df$expo_week=="week3_23","14+",df$days_post_vaccination)
df$prior_covid <- "All"
df$estimate <- df$HR
df$conf.low <- df$lci
df$conf.high <- df$uci
df$events <- df$events_total
df <- df[,c("outcome","nation","dose","age_group","sex","exposure",
            "days_post_vaccination","prior_covid","events","estimate","conf.low","conf.high")]

# Save data --------------------------------------------------------------------

data.table::fwrite(df, "output/wales.csv")