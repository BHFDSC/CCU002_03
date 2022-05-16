rm(list = ls())

library(magrittr)
library(patchwork)

# Load data --------------------------------------------------------------------

df <- data.table::fread("output/estimates.csv", data.table = FALSE)

df <- df[df$prior_covid=="All" & 
           df$nation=="England" & 
           df$outcome=="myocarditis/pericarditis" &
           df$sex=="All" &
           df$age_group=="All",]

# Make dose labels -------------------------------------------------------------

doses <- data.table::fread("output/doses.csv", data.table = FALSE)

dose1_overall <- paste0("Dose 1 \nBNT162b2, N = ",
                        format(doses[doses$analysis_dose=="Dose 1" & doses$sample=="Total" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                        " with ",
                        format(doses[doses$analysis_dose=="Dose 1" & doses$sample=="Exposed" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                        " exposed\nChAdOx1-S, N = ",
                        format(doses[doses$analysis_dose=="Dose 1" & doses$sample=="Total" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                        " with ",
                        format(doses[doses$analysis_dose=="Dose 1" & doses$sample=="Exposed" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                        " exposed")

dose2_overall <- paste0("Dose 2 \nBNT162b2, N = ",
                        format(doses[doses$analysis_dose=="Dose 2" & doses$sample=="Total" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                        " with ",
                        format(doses[doses$analysis_dose=="Dose 2" & doses$sample=="Exposed" & doses$analysis_product=="BNT162b2",]$N, big.mark = ",", scientific = FALSE),
                        " exposed\nChAdOx1-S, N = ",
                        format(doses[doses$analysis_dose=="Dose 2" & doses$sample=="Total" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                        " with ",
                        format(doses[doses$analysis_dose=="Dose 2" & doses$sample=="Exposed" & doses$analysis_product=="ChAdOx1-S",]$N, big.mark = ",", scientific = FALSE),
                        " exposed")

# Order variables --------------------------------------------------------------

df$dose <- factor(df$dose, levels=c("Dose 1", "Dose 2"))

df$exposure <- factor(df$exposure, levels = c("BNT162b2","dummy","ChAdOx1-S"))

df$facet_lab <- as.numeric(df$dose)
df$facet_lab <- factor(df$facet_lab, 
                       levels = 1:2,
                       labels = c(dose1_overall, dose2_overall))

# Make plot element of figure --------------------------------------------------

ggplot2::ggplot(df, 
                mapping = ggplot2::aes(x=days_post_vaccination, 
                                       y=estimate, color=exposure)) +
  ggplot2::geom_hline(yintercept=1, lwd=0.5, col="dark grey") +
  ggplot2::geom_linerange(ggplot2::aes(ymin=conf.low, ymax=conf.high, color=exposure), 
                          position=ggplot2::position_dodge(0.5)) + 
  ggplot2::geom_point(position=ggplot2::position_dodge(0.5), size = 2.5) + 
  ggplot2::labs(x = "Days since vaccination", 
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
                 text = ggplot2::element_text(size = 12),
                 axis.text = ggplot2::element_text(size = 8)) + 
  ggplot2::facet_wrap(facet_lab~., ncol = 2, scales = "free_x")

# Save figure ------------------------------------------------------------------

ggplot2::ggsave(filename = "output/figure_main.jpeg",
                dpi = 600, width = 297, height = 210,
                unit = "mm", scale = 1)