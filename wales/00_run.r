# clear the environment ========================================================
rm(list=ls())
gc()
setwd("P:/torabif/workspace/CCU0002-03")

# load options, packages and functions =========================================

source("01_load.r")

# create data ==================================================================

# Run '02', '03' and '04' scripts in Eclipse

# prepare data for analysis ====================================================

source("03a_save_data.r")
source("04a_sample_selection_summary.r")


# report =======================================================================

# for gitlab
render(
	input = "README.Rmd",
	output_format = md_document(),
	quiet = TRUE
)

# for local viewing
render(
	input = "README.rmd",
	output_file = "README.html",
	quiet = TRUE
)
