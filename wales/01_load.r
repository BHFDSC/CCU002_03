# options ======================================================================

options(
    dplyr.summarise.inform = FALSE
)

# plot settings ================================================================

p_width  <- 5.2
p_height <- 8.75
p_dpi    <- 300

# packages =====================================================================
message("Loading packages:")

pkgs <- c(
    "assertr",
    "beepr",
    "broom",
    "dplyr",
    "dtplyr",
    "forcats",
    "ggplot2",
    "ggthemes",
    "knitr",
    "kableExtra",
    "mice",
    "janitor",
    "lubridate",
    "qs",
    "rmarkdown",
    "sailr",
    "scales",
    "stringr",
    "readr",
    "survival",
    "tableone",
    "tidyr",
    # Fatemeh uses the following:
    "RODBC",
    "Cairo",
    "lattice",
    "getopt",
    "gtsummary"
)

for (pkg in pkgs) {
    suppressWarnings(
        suppressPackageStartupMessages(
            library(pkg, character.only = TRUE)
        )
    )
    message("\t", pkg, sep = "")
}

# custom functions =============================================================

project_dir <- function(...) {
    str_c("S:/0911 - Utilising routine data and machine learning techniques to discover new multi/DacVap/", ...)
}

KableOne <- function(x, ...) {
    k1 <- print(x, quote = TRUE, printToggle = FALSE, ...)
    rownames(k1) <- gsub(pattern = '\\"', replacement = '', rownames(k1))
    colnames(k1) <- gsub(pattern = '\\"', replacement = '', colnames(k1))
    return(k1)
}
