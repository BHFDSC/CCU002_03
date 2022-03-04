rm(list = ls())

# Combined all dosage data -----------------------------------------------------

df <- NULL

for (i in c("Pfizer","AstraZeneca")) {
  for (j in c(1:2)) {
    
    tmp <- data.table::fread(paste0("raw/england/agedist_dose",j,"_",i,".csv"),
                             data.table = FALSE)
    
    tmp$dose <- paste0("Dose ",j)
    tmp$exposure <- i
    
    tmp <- tmp[,c("age","dose","exposure",paste0("dose",j,"_",i),paste0("dose",j,"_Total"))]
    colnames(tmp) <- c("age","dose","exposure","exposed","total")
    df <- rbind(df,tmp)
    
  }
}

# Prepare variables ------------------------------------------------------------

df$dose <- factor(df$dose, levels=c("Dose 1", "Dose 2"))


df$exposure <- factor(df$exposure, 
                      levels = c("Pfizer","AstraZeneca"), 
                      labels = c("BNT162b2","ChAdOx1-S"))

df$age <- factor(df$age, levels = c(12:99,"100+"))

# Plot -------------------------------------------------------------------------

ggplot2::ggplot(data = df, mapping = ggplot2::aes(x = age)) + 
  ggplot2::geom_col(mapping = ggplot2::aes(x = age, y = total, fill = "N")) + 
  ggplot2::geom_col(mapping = ggplot2::aes(x = age, y = exposed, fill = exposure)) +
  ggplot2::scale_fill_manual(values = c('#5ab4ac','#d8b365','dark grey'),
                             breaks = c("BNT162b2","ChAdOx1-S","N"),
                             labels = c("Exposed: BNT162b2","Exposed: ChAdOx1-S","N")) +
  ggplot2::scale_y_continuous(breaks = seq(100000,700000,100000),
                              labels = format(seq(100000,700000,100000), scientific = FALSE)) +
  ggplot2::scale_x_discrete(breaks = c(seq(15,95,5),"100+")) +
  ggplot2::labs(x = "Age", y = "") +
  ggplot2::theme_minimal() +
  ggplot2::theme(panel.grid.major.x = ggplot2::element_blank(),
                 panel.grid.minor = ggplot2::element_blank(),
                 legend.title = ggplot2::element_blank(),
                 legend.position = "bottom") +
  ggplot2::facet_wrap(exposure~dose, ncol = 2, scales = "free_x")

# Save figure ------------------------------------------------------------------

ggplot2::ggsave(filename = "output/figure_agedist.jpeg",
                dpi = 600, width = 297, height = 0.75*210,
                unit = "mm", scale = 1)