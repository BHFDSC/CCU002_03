rm(list = ls())

library(magrittr)
library(patchwork)

# Load data --------------------------------------------------------------------

df <- data.table::fread("output/meta_analysis.csv", data.table = FALSE)
df <- df[df$source=="meta-analysis",]

# Load event counts ------------------------------------------------------------

counts1 <- data.table::fread("raw/England_dose1_events.csv")
counts1 <- aggregate(events_total ~ vac + expo_week, data = counts1, sum, na.rm = TRUE) %>% 
  dplyr::filter(grepl("week", expo_week)) %>% 
  dplyr::filter(vac %in% c("vac_az", "vac_pf"))
counts1$dose <- "Dose 1"

counts2 <- data.table::fread("raw/England_dose2_events.csv")
counts2 <- aggregate(events_total ~ vac + expo_week, data = counts2, sum, na.rm = TRUE) %>% 
  dplyr::filter(grepl("week", expo_week)) %>% 
  dplyr::filter(vac %in% c("vac_az", "vac_pf"))
counts2$dose <- "Dose 2"

counts <- rbind(counts1, counts2) 

df <- dplyr::left_join(df, counts, by = c("term" = "expo_week", "vac_str" = "vac", "dose"="dose"))

# Prepare data ------------------------------------------------------------------

df$term <- factor(df$term,levels=c("week1_2", "week3_23"))
df$term <- dplyr::recode(df$term, "week1_2" = "0-13", "week3_23"="14+")
df$vac_str <- factor(df$vac_str, levels=c("vac_az", "vac_pf"))
df$vac_str <- dplyr::recode(df$vac_str , "vac_az" = "ChAdOx1-S", "vac_pf"="BNT162b2")
df$dose <- factor(df$dose, levels=c("Dose 1", "Dose 2"))

df$info <- paste0(df$events_total," cases\nHR: ",sprintf("%.2f",df$estimate)," (95% CI: ",sprintf("%.2f",df$conf.low),", ",sprintf("%.2f",df$conf.high),")")

# Plot -------------------------------------------------------------------------

p1 <- ggplot2::ggplot(df, 
                mapping = ggplot2::aes(x=term, y=estimate, color=vac_str)) +
  ggplot2::geom_linerange(ggplot2::aes(ymin=conf.low, ymax=conf.high, color=vac_str), size=2, alpha=0.3, position=ggplot2::position_dodge(0.5)) + 
  ggplot2::geom_point(shape = 15, size = 1.5, position=ggplot2::position_dodge(0.5)) + 
  ggplot2::labs(x = "Days since vaccination", 
                y = "Hazard ratio and 95% confidence interval",
                color = "vaccine type") +
  ggplot2::scale_color_manual(values = c("#FF9900", "#339999")) +
  ggplot2::scale_y_continuous(trans = "log", breaks = (2^seq(-1,2)), lim = c(2^-1,2^2)) +
  ggplot2::geom_hline(yintercept=1, lwd=0.5, col="dark grey") +
  ggplot2::theme_minimal() +
  ggplot2::theme(plot.background = ggplot2::element_rect(fill = "white", colour = "white"),
                 panel.grid.major.x = ggplot2::element_blank(),
                 panel.grid.minor = ggplot2::element_blank(),
                 panel.border = ggplot2::element_rect(colour = "grey", fill=NA, size=1),
                 legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                 legend.title = ggplot2::element_blank(),
                 legend.position = "bottom",
                 text = ggplot2::element_text(size = 12),
                 axis.text = ggplot2::element_text(size = 10)) 

p2 <- df %>% 
  ggplot2::ggplot(ggplot2::aes(x = term)) +
  ggplot2::geom_text(ggplot2::aes(y = forcats::fct_rev(vac_str), label = info),  size=3) +
  ggplot2::theme_minimal() +
  ggplot2::labs(y = "", x="") +
  ggplot2::theme(axis.line = ggplot2::element_blank(), 
                 axis.ticks = ggplot2::element_blank(), 
                 axis.text.x = ggplot2::element_blank(),
                 panel.grid = ggplot2::element_blank(),
                 strip.text = ggplot2::element_blank(),
                 text = ggplot2::element_text(size = 12)) 

p_final <- p1 / p2 +  plot_layout(heights = c(8, 2))

(p1 + ggplot2::facet_grid(~dose)) / (p2 + ggplot2::facet_grid(~dose) ) +  plot_layout(heights = c(8, 2))

ggplot2::ggsave(filename = "output/Figure1.jpeg",
                dpi = 300, width = 297, height = 210, 
                unit = "mm", scale = 0.8)