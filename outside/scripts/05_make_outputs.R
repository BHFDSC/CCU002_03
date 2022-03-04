rm(list = ls())

# Make main figure -------------------------------------------------------------

source("scripts/05a_figure_main.R")

# Make prior COVID-19 infection figure -----------------------------------------

source("scripts/05b_figure_priorcovid.R")

# Make outcomes figure ---------------------------------------------------------

source("scripts/05c_figure_outcomes.R")

# Make Welsh figure ------------------------------------------------------------

source("scripts/05d_figure_wales.R")

# Make age distribution figure -------------------------------------------------

source("scripts/05e_figure_agedist.R")

# Make supplement --------------------------------------------------------------

rmarkdown::render('scripts/supplement.Rmd', 
                  output_file =  "supplement.pdf", 
                  output_dir = 'output/')