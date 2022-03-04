rm(list = ls())

library(magrittr)
library(patchwork)

# Load data --------------------------------------------------------------------

df <- data.table::fread("output/wales.csv")
df <- df[df$days_post_vaccination!="Before",]

# Make plot --------------------------------------------------------------------

ggplot2::ggplot(df, mapping = ggplot2::aes(x=days_post_vaccination, y=estimate, color=exposure)) +
  ggplot2::geom_hline(yintercept=1, lwd=0.5, col="dark grey") +
  ggplot2::geom_linerange(ggplot2::aes(ymin=conf.low, ymax=conf.high, color=exposure), 
                          position=ggplot2::position_dodge(0.5)) + 
  ggplot2::geom_point(position=ggplot2::position_dodge(0.5), size = 2.5) + 
  ggplot2::labs(x = "Days since vaccination", 
                y = "Hazard ratio and 95% confidence interval",
                color = "Prior COVID-19 infection") +
  ggplot2::scale_color_manual(values = c('#5ab4ac','#d8b365'), 
                              breaks = c("BNT162b2","ChAdOx1-S")) +
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
                 axis.text = ggplot2::element_text(size = 8))

# Save figure ------------------------------------------------------------------

ggplot2::ggsave(filename = "output/figure_wales.jpeg",
                dpi = 600, width = 210, height = 105,
                unit = "mm", scale = 1)