rm(list = ls())

# Load data --------------------------------------------------------------------

england1 <- readxl::read_excel("raw/England_exposed.xlsx",
                               sheet = "D1")

england2 <- readxl::read_excel("raw/England_exposed.xlsx",
                               sheet = "D2")

wales <- data.table::fread("raw/Wales_exposed.csv", data.table = FALSE)

