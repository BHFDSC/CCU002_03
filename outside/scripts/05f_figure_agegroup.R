rm(list = ls())

library(magrittr)
library(patchwork)

# Load data --------------------------------------------------------------------

df <- data.table::fread("output/estimates.csv", data.table = FALSE)

# Filter to relevant data ------------------------------------------------------

df <- df[df$outcome=="Myocarditis" & 
           df$nation=="England" &
           df$age_group!="All" &
           df$sex=="All" &
           df$prior_covid=="All" &
           df$extended_fup==FALSE,]

df$days <- ifelse(df$term=="day0_14","0-13","14+")

# Make plot --------------------------------------------------------------------

ggplot2::ggplot(df, mapping = ggplot2::aes(x=days, 
                                           y=estimate, 
                                           color=exposure, 
                                           shape=age_group)) +
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
  ggplot2::scale_shape_manual(values = c(1,2,0),
                              breaks = c("<40","40-69","70+"),
                              labels = c("Age group: <40","Age group: 40-69","Age group: 70+")) +
  ggplot2::scale_y_continuous(trans = "log", lim = c(2^-4,2^3), breaks = (2^seq(-4,4)), labels = sprintf("%.2f",(2^seq(-4,4)))) +
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
  ggplot2::facet_wrap(exposure~dose, ncol = 2, scales = "free_x")

# Save figure ------------------------------------------------------------------

ggplot2::ggsave(filename = "output/figure_agegroup.jpeg",
                dpi = 600, width = 210, height = 140,
                unit = "mm", scale = 1)
