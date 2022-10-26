rm(list = ls())

library(magrittr)
library(patchwork)

for (outcome in c("Myocarditis","Pericarditis")) {

  # Load data --------------------------------------------------------------------
  
  df <- data.table::fread("output/estimates.csv", data.table = FALSE)
  
  df <- df[df$prior_covid=="All" & 
             df$nation=="England" & 
             df$outcome==outcome &
             df$sex=="All" &
             df$age_group=="All",]
  
  # Make dose labels -------------------------------------------------------------
  
  doses <- data.table::fread("output/doses.csv", data.table = FALSE)
  doses$extended_fup <- FALSE
  
  doses_extn <- data.table::fread("output/doses.csv", data.table = FALSE)
  doses_extn$extended_fup <- TRUE
  doses_extn$N <- NA
  doses <- rbind(doses, doses_extn)
  
  dose1_overall <- paste0("Dose 1\n\nBNT162b2:\nN = ",
                          format(doses[doses$extended_fup==FALSE & doses$analysis_dose=="Dose 1" & doses$sample=="Total" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==FALSE & doses$analysis_dose=="Dose 1" & doses$sample=="Exposed" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 17-06-21\nN = ",
                          format(doses[doses$extended_fup==TRUE & doses$analysis_dose=="Dose 1" & doses$sample=="Total" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==TRUE & doses$analysis_dose=="Dose 1" & doses$sample=="Exposed" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 23-01-22\n\nChAdOx1-S:\nN = ",
                          format(doses[doses$extended_fup==FALSE & doses$analysis_dose=="Dose 1" & doses$sample=="Total" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==FALSE &doses$analysis_dose=="Dose 1" & doses$sample=="Exposed" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 17-06-21\nN = ",
                          format(doses[doses$extended_fup==TRUE &doses$analysis_dose=="Dose 1" & doses$sample=="Total" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==TRUE &doses$analysis_dose=="Dose 1" & doses$sample=="Exposed" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 23-01-22\n")
  
  dose2_overall <- paste0("Dose 2\n\nBNT162b2:\nN = ",
                          format(doses[doses$extended_fup==FALSE & doses$analysis_dose=="Dose 2" & doses$sample=="Total" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==FALSE & doses$analysis_dose=="Dose 2" & doses$sample=="Exposed" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 17-06-21\nN = ",
                          format(doses[doses$extended_fup==TRUE & doses$analysis_dose=="Dose 2" & doses$sample=="Total" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==TRUE & doses$analysis_dose=="Dose 2" & doses$sample=="Exposed" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 23-01-22\n\nChAdOx1-S:\nN = ",
                          format(doses[doses$extended_fup==FALSE & doses$analysis_dose=="Dose 2" & doses$sample=="Total" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==FALSE &doses$analysis_dose=="Dose 2" & doses$sample=="Exposed" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 17-06-21\nN = ",
                          format(doses[doses$extended_fup==TRUE &doses$analysis_dose=="Dose 2" & doses$sample=="Total" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " with ",
                          format(doses[doses$extended_fup==TRUE &doses$analysis_dose=="Dose 2" & doses$sample=="Exposed" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                          " exposed up to 23-01-22\n")
  
  # Order variables --------------------------------------------------------------
  
  df$dose <- factor(df$dose, levels=c("Dose 1", "Dose 2"))
  
  df$exposure <- factor(df$exposure, levels = c("BNT162b2","dummy","ChAdOx1-S"))
  
  df$facet_lab <- as.numeric(df$dose)
  df$facet_lab <- factor(df$facet_lab, 
                         levels = 1:2,
                         labels = c(dose1_overall, dose2_overall))
  
  
  df$days <- ifelse(df$term=="day0_14","0-13","14+")
  
  df$end <- ifelse(df$extended_fup==TRUE,"Follow-up to 23-01-22","Follow-up to 17-06-21")
  df$end <- factor(df$end, levels = c("Follow-up to 17-06-21","Follow-up to 23-01-22"))
  
  # Make plot element of figure --------------------------------------------------
  
  p1 <- ggplot2::ggplot(df, 
                        mapping = ggplot2::aes(x=days, y=estimate, color=exposure)) +
    ggplot2::geom_hline(yintercept=1, lwd=0.5, col="dark grey") +
    ggplot2::geom_linerange(ggplot2::aes(ymin=conf.low, ymax=conf.high, color=exposure, linetype=end), 
                            position=ggplot2::position_dodge(0.5)) + 
    ggplot2::geom_point(ggplot2::aes(color=exposure, linetype=end),
                        position=ggplot2::position_dodge(0.5), size = 2.5) + 
    ggplot2::labs(x = "Days since vaccination", #"Days since vaccination\n\n\nMyocarditis events following vaccination:", 
                  y = "Hazard ratio and 95% confidence interval",
                  color = "vaccine type") +
    ggplot2::scale_color_manual(values = c('#5ab4ac','#d8b365'), 
                                breaks = c("BNT162b2","ChAdOx1-S"), 
                                labels = c("BNT162b2","ChAdOx1-S")) +
    ggplot2::scale_y_continuous(trans = "log", breaks = (2^seq(-4,4)), labels = sprintf("%.2f",(2^seq(-4,4)))) +
    ggplot2::guides(color=ggplot2::guide_legend(order = 1, ncol=2, byrow=TRUE)) +
    ggplot2::theme_minimal() +
    ggplot2::theme(plot.background = ggplot2::element_rect(fill = "white", colour = "white"),
                   panel.grid.major = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank(),
                   panel.border = ggplot2::element_rect(colour = "grey", fill=NA, size=1),
                   legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                   legend.title = ggplot2::element_blank(),
                   legend.position = "bottom",
                   text = ggplot2::element_text(size = 12)) + 
    ggplot2::facet_wrap(facet_lab~., nrow = 1, scales = "free_x")
  
  # Make table element of figure -------------------------------------------------
  
  # tmp <- df[df$age_group=="All" & df$sex=="All",c("dose","term","extended_fup","exposure","facet_lab")]
  # tmp$exposure <- as.character(tmp$exposure)
  # tmp$exposure <- factor(tmp$exposure, levels = c("BNT162b2","ChAdOx1-S"))
  # tmp$days <- ifelse(tmp$term=="day0_14","0-13","14+")
  # 
  # events <- data.table::fread("output/counts.csv",data.table = FALSE)
  # events <- events[events$outcome=="Myocarditis" & events$prior_covid=="All", 
  #                  c("dose","exposure","extended_fup","term","events")]
  # 
  # events$days <- ifelse(events$term=="day0_14","0-13","14+")
  # 
  # tmp <- merge(tmp, events, by = c("dose","exposure","term","extended_fup","days"))
  # tmp$exposure <- paste0(tmp$exposure, " up to ", ifelse(tmp$extended_fup==TRUE,"23-01-22","17-05-21"))
  # 
  # p2 <- tmp %>% 
  #   ggplot2::ggplot(ggplot2::aes(x = days)) +
  #   ggplot2::geom_text(ggplot2::aes(y = forcats::fct_rev(exposure), label = events),  size=3) +
  #   ggplot2::theme_minimal() +
  #   ggplot2::labs(y = "", x="") +
  #   ggplot2::theme(axis.line = ggplot2::element_blank(), 
  #                  axis.ticks = ggplot2::element_blank(), 
  #                  axis.text.x = ggplot2::element_blank(),
  #                  panel.grid = ggplot2::element_blank(),
  #                  strip.text = ggplot2::element_blank(),
  #                  text = ggplot2::element_text(size = 12)) +
  #   ggplot2::facet_wrap(facet_lab~., ncol = 2)
  
  # Combine figure elements ------------------------------------------------------
  
  #p1 / p2 +  plot_layout(heights = c(12,3), guides = "collect") & ggplot2::theme(legend.position = 'bottom')
  
  p1
  
  # Save figure ------------------------------------------------------------------
  
  ggplot2::ggsave(filename = paste0("output/figure_",outcome,".jpeg"),
                  dpi = 600, width = 297, height = 210,
                  unit = "mm", scale = 0.75)
    
}
