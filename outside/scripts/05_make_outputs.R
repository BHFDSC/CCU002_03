rm(list = ls())

library(magrittr)
library(patchwork)

# Load data --------------------------------------------------------------------

df <- data.table::fread("output/estimates.csv", data.table = FALSE)
df <- df[df$prior_covid=="Mixed" & (df$nation=="All" | (df$nation=="England" & (df$age_group!="All" | df$sex!="All"))),]

# Load event counts ------------------------------------------------------------

counts <- data.table::fread("output/counts.csv",
                            data.table = FALSE)

df <- dplyr::left_join(df, counts, by = c("dose","exposure","days_post_vaccination"))

# Generate table info ----------------------------------------------------------

df$info <- paste0(df$events," events")

# Determine analysis grouping --------------------------------------------------

df$analysis <- "Overall"
df$analysis <- ifelse(df$age_group!="All","Age",df$analysis)
df$analysis <- ifelse(df$sex!="All","Sex",df$analysis)

# Create combination variables for plotting ------------------------------------

df$age_sex <- paste0(df$age_group, "/", df$sex)

# Add dummy points to determine y axis range -----------------------------------

tmp <- data.frame(analysis = rep(c("Overall","Age","Sex"),each = 4),
                  exposure = "dummy",
                  days_post_vaccination = rep(c("0-13","14+"), each = 2),
                  dose = "Dose 1",
                  estimate = c(rep(c(0.9*2^-1,1.1*2^2),2),rep(c(0.85*2^-4,1.15*2^4),2),rep(c(0.85*2^-2,1.15*2^3),2)),
                  age_sex = "All/All")

df <- plyr::rbind.fill(df,tmp)
tmp$dose <- "Dose 2"
df <- plyr::rbind.fill(df,tmp)

# Create facet labels ----------------------------------------------------------

df$facet_lab <- ""
df$facet_lab <- ifelse(df$dose=="Dose 1" & df$analysis=="Overall",1,df$facet_lab)
df$facet_lab <- ifelse(df$dose=="Dose 2" & df$analysis=="Overall",2,df$facet_lab)
df$facet_lab <- ifelse(df$dose=="Dose 1" & df$analysis=="Sex",3,df$facet_lab)
df$facet_lab <- ifelse(df$dose=="Dose 2" & df$analysis=="Sex",4,df$facet_lab)
df$facet_lab <- ifelse(df$dose=="Dose 1" & df$analysis=="Age",5,df$facet_lab)
df$facet_lab <- ifelse(df$dose=="Dose 2" & df$analysis=="Age",6,df$facet_lab)

# Make dose labels -------------------------------------------------------------

doses <- data.table::fread("output/doses.csv", data.table = FALSE)
dose1_overall <- paste0("Dose 1 \nN = ",format(doses[doses$analysis=="Dose 1" & doses$exposure=="Total",]$total, big.mark = ",", scientific = FALSE),"\n\nOverall")
dose2_overall <- paste0("Dose 2 \nN = ",format(doses[doses$analysis=="Dose 2" & doses$exposure=="Total",]$total, big.mark = ",", scientific = FALSE),"\n\nOverall")

# Make dose labels -------------------------------------------------------------

waldtests <- data.table::fread("output/waldtests.csv", data.table = FALSE)
waldtests$p.value <- ifelse(waldtests$p.value<0.01,"p<0.01",paste0("p=",sprintf("%.2f",waldtests$p.value)))

dose1_sex <- paste0("By sex\n\nHeterogeneity p-values:\nBNT162b2, 0-13 ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="14+",]$p.value,
                    "\nChAdOx1-S, 0-13 ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="14+",]$p.value)

dose2_sex <- paste0("By sex\n\nHeterogeneity p-values:\nBNT162b2, 0-13 ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="14+",]$p.value,
                    "\nChAdOx1-S, 0-13 ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Sex" & waldtests$days_post_vaccination=="14+",]$p.value)

dose1_age <- paste0("By age group\n\nHeterogeneity p-values:\nBNT162b2, 0-13 ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="14+",]$p.value,
                    "\nChAdOx1-S, 0-13 ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 1" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="14+",]$p.value)

dose2_age <- paste0("By age group\n\nHeterogeneity p-values:\nBNT162b2, 0-13 ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="BNT162b2" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="14+",]$p.value,
                    "\nChAdOx1-S, 0-13 ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="0-13",]$p.value,
                    ", 14+ ",waldtests[waldtests$dose=="Dose 2" & waldtests$exposure=="ChAdOx1-S" & waldtests$interacting_feature=="Age group" & waldtests$days_post_vaccination=="14+",]$p.value)

# Order variables --------------------------------------------------------------

df$dose <- factor(df$dose, levels=c("Dose 1", "Dose 2"))

df$exposure <- factor(df$exposure, levels = c("BNT162b2","dummy","ChAdOx1-S"))

df$facet_lab <- factor(df$facet_lab, 
                       levels = 1:6,
                       labels = c(dose1_overall, dose2_overall, dose1_sex, dose2_sex, dose1_age, dose2_age))

# Make plot element of figure --------------------------------------------------

p1 <- ggplot2::ggplot(df, mapping = ggplot2::aes(x=days_post_vaccination, y=estimate, color=exposure, shape = age_sex)) +
  ggplot2::geom_hline(yintercept=1, lwd=0.5, col="dark grey") +
  ggplot2::geom_linerange(ggplot2::aes(ymin=conf.low, ymax=conf.high, color=exposure), 
                          position=ggplot2::position_dodge(0.5)) + 
  ggplot2::geom_point(position=ggplot2::position_dodge(0.5), size = 2.5) + 
  ggplot2::labs(x = "Days since vaccination", 
                y = "Hazard ratio and 95% confidence interval",
                color = "vaccine type") +
  ggplot2::scale_color_manual(values = c('#5ab4ac','#d8b365','white'), 
                              breaks = c("BNT162b2","ChAdOx1-S","dummy"), 
                              labels = c("BNT162b2","ChAdOx1-S","")) +
  ggplot2::scale_shape_manual(values = c(16,17,15,1,2,0),
                              breaks = c("All/All","All/Female","All/Male","<40/All","40-69/All","70+/All"),
                              labels = c("Overall","Sex: female","Sex: male","Age group: <40","Age group: 40-69","Age group: 70+")) +
  ggplot2::scale_y_continuous(trans = "log", breaks = (2^seq(-4,4)), labels = sprintf("%.2f",(2^seq(-4,4)))) +
  ggplot2::guides(color=ggplot2::guide_legend(order = 1, ncol=1, byrow=TRUE),
                  shape=ggplot2::guide_legend(ncol=3, byrow=TRUE, order = 2)) +
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
  ggplot2::facet_wrap(facet_lab~., ncol = 2, scales = "free_y")

# Make table element of figure -------------------------------------------------

tmp <- df[df$age_group=="All" & df$sex=="All" & df$exposure!="dummy",c("days_post_vaccination","exposure","info","facet_lab")]
tmp$exposure <- as.character(tmp$exposure)
tmp <- rbind(tmp,c("0-13","Comparator",paste0(counts[counts$days_post_vaccination=="Before" & counts$dose=="Dose 1",]$events," events"),dose1_overall))
tmp <- rbind(tmp,c("0-13","Comparator",paste0(counts[counts$days_post_vaccination=="Before" & counts$dose=="Dose 2",]$events," events"),dose2_overall))
tmp$exposure <- factor(tmp$exposure, levels = c("BNT162b2","ChAdOx1-S","Comparator"))

p2 <- tmp %>% 
  ggplot2::ggplot(ggplot2::aes(x = days_post_vaccination)) +
  ggplot2::geom_text(ggplot2::aes(y = forcats::fct_rev(exposure), label = info),  size=3) +
  ggplot2::theme_minimal() +
  ggplot2::labs(y = "", x="") +
  ggplot2::theme(axis.line = ggplot2::element_blank(), 
                 axis.ticks = ggplot2::element_blank(), 
                 axis.text.x = ggplot2::element_blank(),
                 panel.grid = ggplot2::element_blank(),
                 strip.text = ggplot2::element_blank(),
                 text = ggplot2::element_text(size = 12)) +
  ggplot2::facet_wrap(facet_lab~., ncol = 2)

# Combine figure elements ------------------------------------------------------

p1 / p2 +  plot_layout(heights = c(12,1.5), guides = "collect") & ggplot2::theme(legend.position = 'bottom')

# Save figure ------------------------------------------------------------------

ggplot2::ggsave(filename = "output/figure.jpeg",
                dpi = 600, width = 210, height = 270, #297 
                unit = "mm", scale = 1)

# Make supplement --------------------------------------------------------------

rmarkdown::render('scripts/supplement.Rmd', 
                  output_file =  "supplement.pdf", 
                  output_dir = 'output/')