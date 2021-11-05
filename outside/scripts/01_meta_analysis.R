rm(list = ls())

# Identify files containing estimates ------------------------------------------

files <- c(list.files(path = "raw/", pattern = "England_.*_estimates.csv"),
           list.files(path = "raw/", pattern = "Wales_"))

# Combine files in a single data frame -----------------------------------------

df <- NULL

for (f in files) {
  
  # Load data
  
  tmp <- data.table::fread(paste0("raw/",f),
                           data.table = FALSE)
  
  # For English data, add nation and dose  
  
  if (grepl("England_",f)) {
    tmp$nation <- "England"
    tmp$dose <- paste0("Dose ",gsub(".*?([0-9]+).*", "\\1", f))
  }
  
  # For English interaction data, add CI and term
  
  if (grepl("interaction",f)) {
    tmp$conf.low <- NA
    tmp$conf.high <- NA
    tmp$term <- tmp$contrast
  }
  
  # For Welsh data, add nation, dose and vaccine producer
  
  if (grepl("Wales_",f)) {
    tmp$nation <- "Wales"
    tmp$dose <- paste0("Dose ",gsub(".*?([0-9]+).*", "\\1", f))
    tmp$vac_str <- gsub(".*all_","",gsub(".csv","",f))
  }

  # Tidy data
  
  tmp <- tmp[,c("nation","dose","vac_str","term","fml","estimate","conf.low","conf.high","robust.se","p.value")]
  tmp <- tmp[grepl("week",tmp$term),]
  tmp$source <- f

  # Append to master data frame  
  
  df <- plyr::rbind.fill(df,tmp)
  
}

# Create master meta-analysis data frame ---------------------------------------

df_meta <- unique(df[,c("dose","vac_str","term","fml")])
df_meta$nation <- "all"
df_meta$estimate <- NA
df_meta$conf.low<- NA
df_meta$conf.high <- NA
df_meta$robust.se <- NA
df_meta$p.value <- NA
df_meta$source <- "meta-analysis"

# Meta-analyse by nation -------------------------------------------------------

for (i in 1:nrow(df_meta)) {
  
  tmp <- df[df$dose==df_meta[i,"dose"] &
             df$vac_str==df_meta[i,"vac_str"] &
             df$term==df_meta[i,"term"] &
             df$fml==df_meta[i,"fml"],]
  
  if (nrow(tmp)==2) {
      tmp_meta <- meta::metagen(log(tmp$estimate),tmp$robust.se, sm = "HR")
      df_meta[i,]$estimate <- exp(tmp_meta$TE.fixed)
      df_meta[i,]$conf.low <- exp(tmp_meta$lower.fixed)
      df_meta[i,]$conf.high <- exp(tmp_meta$upper.fixed)
      df_meta[i,]$p.value <- tmp_meta$pval.fixed
      df_meta[i,]$robust.se <- tmp_meta$seTE.fixed
    }
    
}

# Add meta-analysis results to main results ------------------------------------

df <- plyr::rbind.fill(df, df_meta)
df <- df[!is.na(df$estimate),]

# Save -------------------------------------------------------------------------

data.table::fwrite(df,"output/meta_analysis.csv")